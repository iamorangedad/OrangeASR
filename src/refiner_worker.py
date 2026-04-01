import asyncio
import json
import time
import nats
from pymongo import MongoClient
from src.config import Config
from src.logger import setup_logger
from src.refiner import ASRRefiner

logger = setup_logger("refiner-worker")


class RefinerWorker:
    def __init__(self):
        self.nc = None
        self.js = None
        self.mongo_client = None
        self.collection = None
        self.refiner = None
        self.session_buffers = {}
        self.session_timers = {}

    def init_resources(self):
        print("⏳ [Refiner] Initializing resources...")

        self.refiner = ASRRefiner(
            model_name=Config.OLLAMA_MODEL,
            base_url=Config.OLLAMA_BASE_URL,
            timeout=Config.REFINE_TIMEOUT,
        )
        print(f"✅ [Refiner] ASRRefiner initialized (model={Config.OLLAMA_MODEL})")

        try:
            self.mongo_client = MongoClient(
                Config.MONGO_URI, serverSelectionTimeoutMS=5000
            )
            self.mongo_client.server_info()
            db = self.mongo_client[Config.MONGO_DB]
            self.collection = db["refined_data"]
            print("✅ [Refiner] MongoDB connected.")
        except Exception as e:
            print(f"❌ [Refiner] MongoDB connection failed: {e}")

    async def process_msg(self, msg):
        try:
            data = json.loads(msg.data.decode())
            req_id = data.get("req_id", "unknown_id")
            session_id = data.get("session_id", "default_session")
            text = data.get("text", "")

            if not text.strip():
                await msg.ack()
                return

            logger.info(
                f"📥 Received transcript for refinement",
                extra={"req_id": req_id, "session_id": session_id},
            )

            if session_id not in self.session_buffers:
                self.session_buffers[session_id] = []

            self.session_buffers[session_id].append({
                "audio_id": req_id,
                "transcribe_text": text,
                "timestamp": data.get("timestamp", time.time()),
            })

            self._reset_session_timer(session_id)

            if len(self.session_buffers[session_id]) >= Config.REFINE_BATCH_SIZE:
                await self._refine_session(session_id)

            await msg.ack()

        except Exception as e:
            logger.error(f"❌ Error processing message: {e}", exc_info=True)
            await msg.ack()

    def _reset_session_timer(self, session_id):
        if session_id in self.session_timers:
            self.session_timers[session_id].cancel()

        loop = asyncio.get_running_loop()
        self.session_timers[session_id] = loop.call_later(
            Config.REFINE_BUFFER_TTL,
            lambda: asyncio.ensure_future(self._refine_session(session_id)),
        )

    async def _refine_session(self, session_id):
        if session_id not in self.session_buffers or not self.session_buffers[session_id]:
            return

        segments = self.session_buffers.pop(session_id, [])
        if session_id in self.session_timers:
            self.session_timers.pop(session_id, None).cancel()

        if len(segments) < 1:
            return

        try:
            refined = self.refiner.refine(segments)
            logger.info(
                f"✅ Refined session {session_id}: '{refined.get('corrected_text', '')}'",
                extra={"session_id": session_id},
            )
        except Exception as e:
            logger.error(
                f"⚠️ LLM refinement failed for session {session_id}: {e}",
                extra={"session_id": session_id},
            )
            refined = {
                "audio_id": segments[0]["audio_id"],
                "original_text": segments[0]["transcribe_text"],
                "corrected_text": segments[0]["transcribe_text"],
            }

        if self.collection is not None:
            doc = {
                "session_id": session_id,
                "audio_id": refined.get("audio_id", segments[0]["audio_id"]),
                "original_text": refined.get("original_text", ""),
                "corrected_text": refined.get("corrected_text", ""),
                "segments": segments,
                "model_used": Config.OLLAMA_MODEL,
                "refined_at": time.time(),
            }
            try:
                self.collection.insert_one(doc)
                logger.info(
                    f"✅ Saved refined data to DB",
                    extra={"session_id": session_id},
                )
            except Exception as db_e:
                logger.error(
                    f"❌ MongoDB insert failed: {db_e}",
                    extra={"session_id": session_id},
                )

    async def start(self):
        self.init_resources()

        print(f"🔌 [Refiner] Connecting to NATS: {Config.NATS_URL}")
        try:
            self.nc = await nats.connect(Config.NATS_URL)
            self.js = self.nc.jetstream()

            try:
                await self.js.add_stream(name="ASR_REFINER", subjects=["asr.output"])
            except Exception:
                pass

            print("🚀 Refiner Worker started! Listening to 'asr.output'...")

            await self.js.subscribe(
                "asr.output",
                queue="refiner_workers",
                cb=self.process_msg,
                manual_ack=True,
            )

            await asyncio.Future()

        except Exception as e:
            logger.critical(f"❌ Startup failed: {e}", exc_info=True)
        finally:
            for timer in self.session_timers.values():
                timer.cancel()
            if self.nc:
                await self.nc.close()


if __name__ == "__main__":
    worker = RefinerWorker()
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        logger.info("🛑 Refiner Worker stopped.")
