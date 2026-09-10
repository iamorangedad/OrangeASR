import asyncio
import json
import base64
import time
import os
import wave
import numpy as np
import nats
try:
    from faster_whisper import WhisperModel
except ImportError:
    WhisperModel = None
from src.logger import setup_logger
from src.config import Config

DEVICE = os.getenv("ASR_DEVICE", Config.ASR_DEVICE)
COMPUTE_TYPE = os.getenv("ASR_COMPUTE_TYPE", Config.ASR_COMPUTE_TYPE)

logger = setup_logger("asr-worker")


class ASRWorker:
    def __init__(self):
        self.nc = None
        self.js = None
        self.model = None
        self.queue = asyncio.Queue()
        self.sem = asyncio.Semaphore(Config.ASR_MAX_CONCURRENCY)
        self.batch_window = Config.ASR_BATCH_WINDOW_MS / 1000.0

    def load_model(self):
        if WhisperModel is None:
            print("⚠️ faster-whisper not installed, skip model load (test mode)")
            return
        print(f"⏳ [ASR Worker] Loading Whisper Model ({DEVICE}/{COMPUTE_TYPE})...")
        try:
            self.model = WhisperModel("tiny", device=DEVICE, compute_type=COMPUTE_TYPE)
            print("✅ [ASR Worker] Model Loaded successfully!")
            if Config.ASR_WARMUP:
                try:
                    print("🔥 Warmup inference with 1s silence...")
                    dummy = np.zeros(16000, dtype=np.float32)
                    list(self.model.transcribe(dummy, beam_size=1, language="en", vad_filter=True, vad_parameters=dict(min_silence_duration_ms=500))[0])
                    print("✅ Warmup done")
                except Exception as e:
                    print(f"⚠️ Warmup failed: {e}")
        except Exception as e:
            print(f"❌ [ASR Worker] CRITICAL: Model load failed - {e}")
            exit(1)

    def run_inference(self, audio_np, previous_text="", req_id="N/A"):
        if not self.model:
            return ""
        max_amp = np.max(np.abs(audio_np))
        avg_amp = np.mean(np.abs(audio_np))
        if max_amp < 0.005:
            logger.warning(
                f"🔇 Audio is too quiet (Max: {max_amp:.4f}). VAD will likely ignore it.",
                extra={"req_id": req_id, "vol_max": float(max_amp)},
            )
            return ""
        logger.info(
            f"🎤 Processing Audio: MaxVol={max_amp:.3f}, AvgVol={avg_amp:.3f}",
            extra={"req_id": req_id},
        )
        try:
            segments_gen, info = self.model.transcribe(
                audio_np,
                beam_size=1,
                language="en",
                initial_prompt=previous_text,
                condition_on_previous_text=True,
                vad_filter=True,
                vad_parameters=dict(min_silence_duration_ms=500),
            )
            segments = list(segments_gen)
            if not segments:
                logger.warning(
                    f"⚠️ Whisper finished but found NO segments. (VAD likely filtered it out)",
                    extra={"req_id": req_id},
                )
                return ""
            result_text = "".join([s.text for s in segments])
            return result_text
        except Exception as e:
            logger.error(f"Inference Error: {e}", extra={"req_id": req_id})
            return ""

    async def _infer_with_sem(self, audio_float32, previous_text, req_id):
        async with self.sem:
            loop = asyncio.get_running_loop()
            try:
                new_text = await asyncio.wait_for(
                    loop.run_in_executor(None, self.run_inference, audio_float32, previous_text, req_id),
                    timeout=Config.ASR_INFERENCE_TIMEOUT,
                )
                return new_text
            except asyncio.TimeoutError:
                logger.error(f"⏰ Inference timeout 5s", extra={"req_id": req_id})
                return ""
            except Exception as e:
                logger.error(f"Inference Error: {e}", extra={"req_id": req_id})
                return ""

    async def _process_one(self, item):
        msg, payload, audio_bytes, start_time = item
        req_id = payload.get("req_id", "unknown")
        session_id = payload.get("session_id", "unknown")
        previous_text = payload.get("previous_text", "")
        try:
            audio_int16 = np.frombuffer(audio_bytes, dtype=np.int16)
            audio_float32 = audio_int16.astype(np.float32) / 32768.0
            new_text = await self._infer_with_sem(audio_float32, previous_text, req_id)
            latency = round(time.time() - start_time, 3)
            if new_text.strip():
                logger.info(
                    f"✅ Result: '{new_text}'",
                    extra={
                        "req_id": req_id,
                        "session_id": session_id,
                        "latency": latency,
                    },
                )
                output_payload = {
                    "req_id": req_id,
                    "session_id": session_id,
                    "text": new_text,
                    "latency": latency,
                    "timestamp": time.time(),
                    "audio_b64": payload.get("audio_b64", base64.b64encode(audio_bytes).decode() if audio_bytes else ""),
                }
                try:
                    await self.js.publish("asr.output", json.dumps(output_payload).encode())
                except Exception as e:
                    logger.error(f"Publish asr.output failed: {e}", extra={"req_id": req_id})
            else:
                logger.warning(
                    f"🚫 No Result (Silence or Unclear)", extra={"req_id": req_id}
                )
            try:
                await msg.ack()
            except Exception:
                pass
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}", exc_info=True)
            try:
                await msg.ack()
            except Exception:
                pass

    async def batch_loop(self):
        while True:
            try:
                first = await self.queue.get()
                batch = [first]
                await asyncio.sleep(self.batch_window)
                while not self.queue.empty() and len(batch) < 4:
                    try:
                        batch.append(self.queue.get_nowait())
                    except Exception:
                        break
                await asyncio.gather(*[self._process_one(it) for it in batch])
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"batch_loop error: {e}", exc_info=True)

    async def process_msg(self, msg):
        try:
            payload = {}
            audio_bytes = b""
            headers = getattr(msg, "headers", None) or {}
            if msg.data and len(msg.data) > 0:
                try:
                    payload = json.loads(msg.data.decode())
                except Exception:
                    payload = {}
            if payload.get("audio_b64"):
                try:
                    audio_bytes = base64.b64decode(payload["audio_b64"])
                except Exception:
                    audio_bytes = b""
            elif msg.data and not payload.get("audio_b64"):
                if headers and headers.get("req_id"):
                    audio_bytes = msg.data
                    payload = {
                        "req_id": headers.get("req_id", "unknown"),
                        "session_id": headers.get("session_id", "unknown"),
                        "previous_text": headers.get("previous_text", ""),
                        "timestamp": float(headers.get("timestamp", time.time())),
                        "capture_ts": headers.get("capture_ts", ""),
                    }
                else:
                    try:
                        if msg.data:
                            audio_bytes = base64.b64decode(payload.get("audio_b64", "")) if payload.get("audio_b64") else msg.data
                    except Exception:
                        audio_bytes = msg.data
            else:
                if headers and headers.get("req_id"):
                    audio_bytes = msg.data or b""
                    payload = {
                        "req_id": headers.get("req_id", "unknown"),
                        "session_id": headers.get("session_id", "unknown"),
                        "previous_text": headers.get("previous_text", ""),
                        "timestamp": float(headers.get("timestamp", time.time())),
                    }
            if not payload.get("req_id"):
                payload["req_id"] = headers.get("req_id", "unknown") if headers else "unknown"
                payload["session_id"] = headers.get("session_id", "unknown") if headers else "unknown"
                payload["previous_text"] = headers.get("previous_text", "") if headers else ""
            if len(audio_bytes) == 0 and payload.get("audio_b64"):
                try:
                    audio_bytes = base64.b64decode(payload["audio_b64"])
                except Exception:
                    pass
            start_time = time.time()
            await self.queue.put((msg, payload, audio_bytes, start_time))
        except Exception as e:
            logger.error(f"❌ Error queueing message: {e}", exc_info=True)
            try:
                await msg.ack()
            except Exception:
                pass

    async def start(self):
        self.load_model()
        print(f"🔌 Connecting to NATS: {Config.NATS_URL}")
        try:
            self.nc = await nats.connect(Config.NATS_URL)
            self.js = self.nc.jetstream()
            try:
                await self.js.add_stream(name="ASR_INPUT", subjects=["asr.input"])
            except Exception:
                pass
            asyncio.create_task(self.batch_loop())
            await self.js.subscribe(
                "asr.input", queue="asr_workers", cb=self.process_msg, manual_ack=True
            )
            print("🚀 ASR Worker started! Waiting for audio chunks...")
            await asyncio.Future()
        except Exception as e:
            print(f"Startup failed: {e}")
        finally:
            if self.nc:
                await self.nc.close()


if __name__ == "__main__":
    worker = ASRWorker()
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        print("🛑 Worker stopped.")
