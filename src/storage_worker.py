import asyncio
import json
import base64
import time
import os
import nats
import boto3
from pymongo import MongoClient
from io import BytesIO
import wave
from src.config import Config
from src.logger import setup_logger


logger = setup_logger("storage-worker")


class StorageWorker:
    def __init__(self):
        self.s3 = None
        self.mongo_client = None
        self.collection = None
        self.nc = None
        self.js = None

    def init_resources(self):
        """Initialize Database and Object Storage connections"""
        print("⏳ [Storage] Initializing resources...")

        # 1. S3 / MinIO
        try:
            self.s3 = boto3.client(
                "s3",
                endpoint_url=Config.S3_ENDPOINT,
                aws_access_key_id=Config.S3_ACCESS_KEY,
                aws_secret_access_key=Config.S3_SECRET_KEY,
            )
            # Connectivity check
            self.s3.list_buckets()
            print("✅ [Storage] MinIO connected.")
        except Exception as e:
            print(f"❌ [Storage] MinIO connection failed: {e}")
            # In production, you might want to exit: exit(1)

        # 2. MongoDB
        try:
            self.mongo_client = MongoClient(
                Config.MONGO_URI, serverSelectionTimeoutMS=5000
            )
            # Connectivity check
            self.mongo_client.server_info()

            db = self.mongo_client[Config.MONGO_DB]
            self.collection = db["transcriptions"]
            print("✅ [Storage] MongoDB connected.")
        except Exception as e:
            print(f"❌ [Storage] MongoDB connection failed: {e}")

    def add_wav_header(self, pcm_bytes):
        """Wraps raw PCM bytes into a valid WAV file in memory"""
        with BytesIO() as wav_buffer:
            with wave.open(wav_buffer, "wb") as wav_file:
                wav_file.setnchannels(1)  # Mono
                wav_file.setsampwidth(2)  # 16-bit (2 bytes)
                wav_file.setframerate(16000)  # 16kHz
                wav_file.writeframes(pcm_bytes)
            return wav_buffer.getvalue()

    async def process_msg(self, msg):
        """
        Handle messages from 'asr.output'
        """
        try:
            data = json.loads(msg.data.decode())

            req_id = data.get("req_id", "unknown_id")
            session_id = data.get("session_id", "default_session")

            # Log receipt (This was commented out in your code)
            logger.info(
                f"📥 Archiving result...",
                extra={"req_id": req_id, "session_id": session_id},
            )

            # ---------------------------------------------------------
            # 1. Process Audio (Upload to MinIO)
            # ---------------------------------------------------------
            s3_key = ""
            if "audio_b64" in data and data["audio_b64"] and self.s3:
                try:
                    # Base64 -> Bytes
                    raw_pcm_bytes = base64.b64decode(data["audio_b64"])

                    # Add wav header
                    wav_bytes = self.add_wav_header(raw_pcm_bytes)

                    # Path: yyyy/mm/dd/session_id/req_id.wav
                    date_prefix = time.strftime("%Y/%m/%d")
                    s3_key = f"{date_prefix}/{session_id}/{req_id}.wav"

                    # Upload
                    self.s3.upload_fileobj(
                        BytesIO(wav_bytes),
                        Config.S3_BUCKET,
                        s3_key,
                        ExtraArgs={"ContentType": "audio/wav"},
                    )
                    # logger.debug(f"S3 Upload Success: {s3_key}", extra={"req_id": req_id})
                except Exception as s3_e:
                    logger.error(
                        f"⚠️ S3 Upload Failed: {s3_e}", extra={"req_id": req_id}
                    )

            # ---------------------------------------------------------
            # 2. Process Metadata (Save to MongoDB)
            # ---------------------------------------------------------
            if self.collection is not None:
                # Remove heavy audio data
                if "audio_b64" in data:
                    del data["audio_b64"]

                data["s3_key"] = s3_key
                data["s3_bucket"] = Config.S3_BUCKET
                data["archived_at"] = time.time()

                self.collection.insert_one(data)

                logger.info(
                    f"✅ Saved to DB", extra={"req_id": req_id, "s3_key": s3_key}
                )
            else:
                logger.warning(
                    "⚠️ DB not connected, skipping insert", extra={"req_id": req_id}
                )

            # ---------------------------------------------------------
            # 3. Acknowledge
            # ---------------------------------------------------------
            await msg.ack()

        except Exception as e:
            logger.error(f"❌ Critical Error processing msg: {e}", exc_info=True)
            # Decide whether to nak() or ack() based on error type
            await msg.nak()

    async def start(self):
        # 1. Initialize DB/S3 first
        self.init_resources()

        # 2. Connect to NATS
        print(f"🔌 [Storage] Connecting to NATS: {Config.NATS_URL}")
        try:
            self.nc = await nats.connect(Config.NATS_URL)
            self.js = self.nc.jetstream()

            # Create Stream if missing (Optional, usually done by setup scripts)
            try:
                await self.js.add_stream(name="ASR_ARCHIVE", subjects=["asr.output"])
            except Exception:
                pass

            print("🚀 Storage Worker started, listening to 'asr.output'...")

            # 3. Subscribe with Queue Group
            await self.js.subscribe(
                "asr.output",
                queue="storage_workers",
                cb=self.process_msg,
                manual_ack=True,
            )

            # Keep running
            await asyncio.Future()

        except Exception as e:
            logger.critical(f"❌ Startup failed: {e}", exc_info=True)
        finally:
            if self.nc:
                await self.nc.close()


if __name__ == "__main__":
    worker = StorageWorker()
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        logger.info("🛑 Storage Worker stopped.")
