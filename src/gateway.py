import asyncio
import json
import base64
import time
import uuid
import wave
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
import nats
from src.config import Config
from src.logger import setup_logger

logger = setup_logger("gateway")


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
    audio_buffer = bytearray()
    SAMPLE_RATE = 16000
    BYTES_PER_SEC = SAMPLE_RATE * 2
    THRESHOLD_BYTES = int(BYTES_PER_SEC * 2.0)
    try:
        while True:
            data = await websocket.receive_bytes()
            audio_buffer.extend(data)
            if len(audio_buffer) >= THRESHOLD_BYTES:
                prompt_text = manager.get_history(session_id)
                req_id = str(uuid.uuid4())
                buffer_size = len(audio_buffer)
                payload = {
                    "req_id": req_id,
                    "session_id": session_id,
                    "audio_b64": base64.b64encode(audio_buffer).decode("utf-8"),
                    "previous_text": prompt_text,
                    "timestamp": time.time(),
                }
                if server_state["js"]:
                    await server_state["js"].publish(
                        "asr.input", json.dumps(payload).encode()
                    )
                    logger.info(
                        f"🚀 Published Audio Chunk ({buffer_size} bytes) to NATS",
                        extra={"req_id": req_id, "session_id": session_id},
                    )
                else:
                    logger.error(
                        "❌ NATS JetStream is not available!",
                        extra={"session_id": session_id},
                    )
                audio_buffer.clear()

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
