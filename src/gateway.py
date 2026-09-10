import asyncio
import json
import base64
import time
import uuid
import collections
import wave
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
import nats
from src.config import Config
from src.logger import setup_logger

logger = setup_logger("gateway")

try:
    import webrtcvad
    HAS_VAD = True
except ImportError:
    webrtcvad = None
    HAS_VAD = False


class VADChunker:
    def __init__(self, sample_rate=16000, min_chunk=0.8, max_chunk=1.5, overlap=0.3, aggressiveness=2, vad_disable=False):
        self.sample_rate = sample_rate
        self.min_bytes = int(min_chunk * sample_rate * 2)
        self.max_bytes = int(max_chunk * sample_rate * 2)
        self.overlap_bytes = int(overlap * sample_rate * 2)
        self.vad_disable = vad_disable or not HAS_VAD
        self.buf = bytearray()
        self.vad = None
        if not self.vad_disable and HAS_VAD:
            try:
                self.vad = webrtcvad.Vad(int(aggressiveness))
            except Exception:
                self.vad = None
                self.vad_disable = True
        self.frame_ms = 30
        self.frame_bytes = int(sample_rate * 2 * self.frame_ms / 1000)
        self.min_silence_ms = 300
        self.silence_frames_needed = max(1, self.min_silence_ms // self.frame_ms)

    def _has_silence(self):
        if len(self.buf) < self.min_bytes:
            return False
        if self.vad_disable or self.vad is None:
            tail_len = int(self.sample_rate * 2 * 0.3)
            tail = self.buf[-tail_len:] if len(self.buf) >= tail_len else self.buf
            if len(tail) == 0:
                return False
            import numpy as np
            arr = np.frombuffer(tail, dtype=np.int16).astype(np.float32)
            rms = float((arr ** 2).mean() ** 0.5) if len(arr) else 0
            return rms < 500
        n_frames = len(self.buf) // self.frame_bytes
        if n_frames < self.silence_frames_needed:
            return False
        silent = 0
        for i in range(n_frames - self.silence_frames_needed, n_frames):
            frame = bytes(self.buf[i * self.frame_bytes:(i + 1) * self.frame_bytes])
            try:
                is_speech = self.vad.is_speech(frame, self.sample_rate)
            except Exception:
                is_speech = True
            if not is_speech:
                silent += 1
            else:
                silent = 0
        return silent >= self.silence_frames_needed

    def feed(self, data: bytes):
        self.buf.extend(data)
        chunks = []
        while True:
            if len(self.buf) >= self.max_bytes:
                cut = self.max_bytes
                chunk = bytes(self.buf[:cut])
                chunks.append(chunk)
                keep = max(0, cut - self.overlap_bytes)
                self.buf = self.buf[keep:] if self.overlap_bytes > 0 else bytearray(self.buf[cut:])
                continue
            if len(self.buf) >= self.min_bytes and self._has_silence():
                cut = len(self.buf)
                if cut > self.max_bytes:
                    cut = self.max_bytes
                chunk = bytes(self.buf[:cut])
                chunks.append(chunk)
                keep = max(0, cut - self.overlap_bytes)
                self.buf = self.buf[keep:] if self.overlap_bytes > 0 else bytearray(self.buf[cut:])
                continue
            break
        return chunks

    def flush(self):
        if len(self.buf) >= self.min_bytes // 2:
            chunk = bytes(self.buf)
            self.buf = bytearray()
            return [chunk]
        return []


class TokenBucket:
    def __init__(self, rate, capacity=None):
        self.rate = float(rate)
        self.capacity = float(capacity or rate)
        self.tokens = self.capacity
        self.last = time.monotonic()

    def consume(self, n=1):
        now = time.monotonic()
        elapsed = now - self.last
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last = now
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False


class ConnectionManager:
    def __init__(self):
        self.active_sessions = {}

    async def connect(self, session_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_sessions[session_id] = {
            "ws": websocket,
            "history": "",
        }
        logger.info(f"✅ WebSocket session accepted.", extra={"session_id": session_id})

    def disconnect(self, session_id: str):
        if session_id in self.active_sessions:
            del self.active_sessions[session_id]
            logger.info(
                f"🔌 WebSocket session removed.", extra={"session_id": session_id}
            )

    async def send_text(self, session_id: str, text: str, latency: float):
        if session_id in self.active_sessions:
            ws = self.active_sessions[session_id]["ws"]
            try:
                await ws.send_json({"type": "update", "text": text, "latency": latency})
                logger.info(
                    f"📤 Sent update to client: '{text}' (Latency: {latency:.3f}s)",
                    extra={"session_id": session_id},
                )
            except Exception as e:
                logger.warning(
                    f"⚠️ Failed to send to client: {e}", extra={"session_id": session_id}
                )
                pass

    async def send_busy(self, session_id: str, pending: int):
        if session_id in self.active_sessions:
            ws = self.active_sessions[session_id]["ws"]
            try:
                await ws.send_json({"type": "busy", "pending": pending})
            except Exception:
                pass

    async def send_backpressure(self, session_id: str, pending: int):
        if session_id in self.active_sessions:
            ws = self.active_sessions[session_id]["ws"]
            try:
                await ws.send_json({"type": "backpressure", "pending": pending})
            except Exception:
                pass

    def update_history(self, session_id: str, new_text: str):
        if session_id in self.active_sessions:
            current = self.active_sessions[session_id]["history"]
            updated = current + new_text
            self.active_sessions[session_id]["history"] = updated[-200:]

    def get_history(self, session_id: str):
        return self.active_sessions.get(session_id, {}).get("history", "")


manager = ConnectionManager()
server_state = {"nc": None, "js": None}


async def handle_asr_result(msg):
    try:
        data = json.loads(msg.data.decode())
        session_id = data.get("session_id")
        req_id = data.get("req_id", "N/A")
        text = data.get("text")
        latency = data.get("latency", 0)
        logger.info(
            f"📥 Received ASR Result via NATS: '{text}'",
            extra={"session_id": session_id, "req_id": req_id},
        )
        if session_id and text:
            manager.update_history(session_id, text)
            await manager.send_text(session_id, text, latency)
        else:
            logger.debug(
                "Received empty or invalid payload", extra={"session_id": session_id}
            )
        await msg.ack()
    except Exception as e:
        logger.error(f"❌ Gateway Error handling NATS msg: {e}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"🔌 [Gateway] Connecting to NATS: {Config.NATS_URL} ...")
    try:
        server_state["nc"] = await nats.connect(Config.NATS_URL)
        server_state["js"] = server_state["nc"].jetstream()
        print("✅ [Gateway] NATS Connected successfully")
        await server_state["js"].subscribe(
            "asr.output",
            cb=handle_asr_result,
            durable="gateway_router",
        )
        print("✅ [Gateway] Listening for 'asr.output'...")
    except Exception as e:
        print(f"❌ [Gateway] NATS Connection Failed: {e}")
    yield
    print("🛑 [Gateway] Shutting down...")
    if server_state["nc"]:
        await server_state["nc"].close()


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws/realtime")
async def websocket_endpoint(websocket: WebSocket):
    session_id = str(uuid.uuid4())
    logger.info(f"🔌 Client connecting.", extra={"session_id": session_id})
    await manager.connect(session_id, websocket)
    chunker = VADChunker(
        sample_rate=16000,
        min_chunk=Config.GATEWAY_MIN_CHUNK_SEC,
        max_chunk=Config.GATEWAY_MAX_CHUNK_SEC,
        overlap=Config.GATEWAY_OVERLAP_SEC,
        aggressiveness=Config.GATEWAY_VAD_AGGRESSIVENESS,
        vad_disable=Config.VAD_DISABLE,
    )
    bucket = TokenBucket(rate=Config.GATEWAY_RATE_LIMIT_RPS)
    pending = 0
    max_pending = Config.GATEWAY_MAX_PENDING
    use_binary = Config.USE_BINARY_PAYLOAD
    try:
        while True:
            data = await websocket.receive_bytes()
            chunks = chunker.feed(data)
            for chunk in chunks:
                if not bucket.consume():
                    pending += 1
                    await manager.send_busy(session_id, pending)
                    logger.warning(f"⏳ Rate limited, busy sent", extra={"session_id": session_id})
                    continue
                if pending >= max_pending:
                    await manager.send_backpressure(session_id, pending)
                    logger.warning(f"⚠️ Backpressure pending={pending}", extra={"session_id": session_id})
                    pending = max(0, pending - 1)
                    continue
                prompt_text = manager.get_history(session_id)
                req_id = str(uuid.uuid4())
                ts = time.time()
                try:
                    if server_state["js"] is None:
                        logger.error("❌ NATS JetStream is not available!", extra={"session_id": session_id})
                        continue
                    if use_binary:
                        headers = {
                            "req_id": req_id,
                            "session_id": session_id,
                            "timestamp": str(ts),
                            "previous_text": prompt_text[:200],
                            "capture_ts": str(ts),
                        }
                        ack = await asyncio.wait_for(
                            server_state["js"].publish(
                                "asr.input", chunk, headers=headers
                            ),
                            timeout=2.0,
                        )
                    else:
                        payload = {
                            "req_id": req_id,
                            "session_id": session_id,
                            "audio_b64": base64.b64encode(chunk).decode("utf-8"),
                            "previous_text": prompt_text,
                            "timestamp": ts,
                        }
                        ack = await asyncio.wait_for(
                            server_state["js"].publish("asr.input", json.dumps(payload).encode()),
                            timeout=2.0,
                        )
                    pending = max(0, pending - 1) if pending > 0 else 0
                    logger.info(
                        f"🚀 Published Audio Chunk ({len(chunk)} bytes) to NATS ack={bool(ack)}",
                        extra={"req_id": req_id, "session_id": session_id},
                    )
                except asyncio.TimeoutError:
                    pending += 1
                    logger.error(f"⏰ Publish ack timeout", extra={"req_id": req_id, "session_id": session_id})
                    await manager.send_busy(session_id, pending)
                except Exception as e:
                    pending += 1
                    logger.error(f"❌ Publish failed: {e}", extra={"session_id": session_id}, exc_info=True)
                    if "slow consumer" in str(e).lower() or "pending" in str(e).lower():
                        await manager.send_backpressure(session_id, pending)

    except WebSocketDisconnect:
        logger.info(
            f"👋 Client disconnected: {session_id}", extra={"session_id": session_id}
        )
        manager.disconnect(session_id)
    except Exception as e:
        logger.error(
            f"❌ WebSocket Error: {e}", extra={"session_id": session_id}, exc_info=True
        )
        manager.disconnect(session_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=Config.API_HOST, port=Config.API_PORT)
