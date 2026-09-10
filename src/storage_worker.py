import asyncio
import json
import base64
import time
import os
import nats
try:
    import aioboto3
    HAS_AIOBOTO = True
except ImportError:
    aioboto3 = None
    HAS_AIOBOTO = False
import boto3
try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    AsyncIOMotorClient = None
    HAS_MOTOR = False
from pymongo import MongoClient
from io import BytesIO
import wave
from src.config import Config
from src.logger import setup_logger
from src import metrics

logger = setup_logger("storage-worker")


class StorageWorker:
    def __init__(self):
        self.s3 = None
        self.s3_async = None
        self.s3_session = None
        self.mongo_client = None
        self.mongo_async_client = None
        self.collection = None
        self.async_collection = None
        self.nc = None
        self.js = None
        self._buffer = []
        self._buffer_lock = asyncio.Lock()
        self._last_flush = time.time()
        self._flush_interval = 1.0
        self._buffer_max = 10
        self._bytesio_pool = []
        self._pool_lock = threading_lock = None

    def _get_bytesio(self, data: bytes):
        if self._bytesio_pool:
            bio = self._bytesio_pool.pop()
            bio.seek(0)
            bio.truncate(0)
            bio.write(data)
            bio.seek(0)
            return bio
        return BytesIO(data)

    def _return_bytesio(self, bio):
        if len(self._bytesio_pool) < 5:
            self._bytesio_pool.append(bio)

    def init_resources(self):
        print("⏳ [Storage] Initializing resources...")
        try:
            self.s3 = boto3.client(
                "s3",
                endpoint_url=Config.S3_ENDPOINT,
                aws_access_key_id=Config.S3_ACCESS_KEY,
                aws_secret_access_key=Config.S3_SECRET_KEY,
            )
            self.s3.list_buckets()
            print("✅ [Storage] MinIO connected.")
        except Exception as e:
            print(f"❌ [Storage] MinIO connection failed: {e}")
        if HAS_AIOBOTO:
            try:
                self.s3_session = aioboto3.Session()
                print("✅ [Storage] aioboto3 available.")
            except Exception:
                pass
        try:
            self.mongo_client = MongoClient(
                Config.MONGO_URI, serverSelectionTimeoutMS=5000
            )
            self.mongo_client.server_info()
            db = self.mongo_client[Config.MONGO_DB]
            self.collection = db["transcriptions"]
            print("✅ [Storage] MongoDB connected.")
        except Exception as e:
            print(f"❌ [Storage] MongoDB connection failed: {e}")
        if HAS_MOTOR:
            try:
                self.mongo_async_client = AsyncIOMotorClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
                self.async_collection = self.mongo_async_client[Config.MONGO_DB]["transcriptions"]
                print("✅ [Storage] Motor async Mongo available.")
            except Exception as e:
                print(f"⚠️ Motor init failed: {e}")
        import threading
        self._pool_lock = threading.Lock()

    def add_wav_header(self, pcm_bytes):
        with BytesIO() as wav_buffer:
            with wave.open(wav_buffer, "wb") as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(16000)
                wav_file.writeframes(pcm_bytes)
            return wav_buffer.getvalue()

    async def _upload_s3(self, wav_bytes, s3_key):
        t0 = time.time()
        try:
            if HAS_AIOBOTO and self.s3_session is not None:
                try:
                    async with self.s3_session.client(
                        "s3",
                        endpoint_url=Config.S3_ENDPOINT,
                        aws_access_key_id=Config.S3_ACCESS_KEY,
                        aws_secret_access_key=Config.S3_SECRET_KEY,
                    ) as s3:
                        await s3.upload_fileobj(BytesIO(wav_bytes), Config.S3_BUCKET, s3_key, ExtraArgs={"ContentType": "audio/wav"})
                    try:
                        metrics.s3_upload_seconds.observe(time.time() - t0)
                    except Exception:
                        pass
                    return True
                except Exception as e:
                    logger.warning(f"aioboto3 upload failed, fallback to sync: {e}")
            loop = asyncio.get_running_loop()
            def _sync_upload():
                bio = BytesIO(wav_bytes)
                self.s3.upload_fileobj(bio, Config.S3_BUCKET, s3_key, ExtraArgs={"ContentType": "audio/wav"})
            await loop.run_in_executor(None, _sync_upload)
            try:
                metrics.s3_upload_seconds.observe(time.time() - t0)
            except Exception:
                pass
            return True
        except Exception as e:
            try:
                metrics.s3_upload_seconds.observe(time.time() - t0)
            except Exception:
                pass
            raise e

    async def _flush_buffer(self):
        async with self._buffer_lock:
            if not self._buffer:
                return
            docs = self._buffer[:]
            self._buffer = []
            self._last_flush = time.time()
        if not docs:
            return
        try:
            if HAS_MOTOR and self.async_collection is not None:
                await self.async_collection.insert_many(docs, ordered=False)
            elif self.collection is not None:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, lambda: self.collection.insert_many(docs, ordered=False))
            logger.info(f"✅ Bulk inserted {len(docs)} docs")
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            for d in docs:
                try:
                    if self.collection is not None:
                        self.collection.insert_one(d)
                except Exception:
                    pass

    async def _periodic_flush(self):
        while True:
            await asyncio.sleep(self._flush_interval)
            if self._buffer and (time.time() - self._last_flush >= self._flush_interval):
                await self._flush_buffer()

    async def process_msg(self, msg):
        try:
            headers = getattr(msg, "headers", None) or {}
            try:
                data = json.loads(msg.data.decode())
            except Exception:
                data = {}
                if headers.get("req_id"):
                    data = {
                        "req_id": headers.get("req_id"),
                        "session_id": headers.get("session_id", "default_session"),
                        "text": headers.get("text", ""),
                        "timestamp": float(headers.get("timestamp", time.time())),
                    }
                    pcm = msg.data or b""
                    if pcm:
                        try:
                            json.loads(pcm.decode())
                        except Exception:
                            data["audio_b64"] = base64.b64encode(pcm).decode()
            if not data.get("req_id") and headers.get("req_id"):
                data["req_id"] = headers.get("req_id")
                data["session_id"] = headers.get("session_id", data.get("session_id", "default_session"))
            if not data.get("audio_b64") and headers.get("req_id") and msg.data and len(msg.data) > 100:
                try:
                    json.loads(msg.data.decode())
                except Exception:
                    data["audio_b64"] = base64.b64encode(msg.data).decode()

            req_id = data.get("req_id", "unknown_id")
            session_id = data.get("session_id", "default_session")

            logger.info(
                f"📥 Archiving result...",
                extra={"req_id": req_id, "session_id": session_id},
            )

            s3_key = ""
            if "audio_b64" in data and data["audio_b64"] and self.s3:
                try:
                    raw_pcm_bytes = base64.b64decode(data["audio_b64"])
                    wav_bytes = await asyncio.get_running_loop().run_in_executor(None, self.add_wav_header, raw_pcm_bytes)
                    date_prefix = time.strftime("%Y/%m/%d")
                    s3_key = f"{date_prefix}/{session_id}/{req_id}.wav"
                    await self._upload_s3(wav_bytes, s3_key)
                except Exception as s3_e:
                    logger.error(
                        f"⚠️ S3 Upload Failed: {s3_e}", extra={"req_id": req_id}
                    )

            if self.collection is not None or self.async_collection is not None:
                if "audio_b64" in data:
                    del data["audio_b64"]
                data["s3_key"] = s3_key
                data["s3_bucket"] = Config.S3_BUCKET
                data["archived_at"] = time.time()
                async with self._buffer_lock:
                    self._buffer.append(data)
                    need_flush = len(self._buffer) >= self._buffer_max
                if need_flush:
                    await self._flush_buffer()
                else:
                    logger.info(
                        f"📦 Buffered for bulk insert ({len(self._buffer)}/{self._buffer_max})", extra={"req_id": req_id}
                    )
            else:
                logger.warning(
                    "⚠️ DB not connected, skipping insert", extra={"req_id": req_id}
                )

            await msg.ack()

        except Exception as e:
            logger.error(f"❌ Critical Error processing msg: {e}", exc_info=True)
            await msg.nak()

    async def start(self):
        self.init_resources()
        try:
            metrics.start_metrics_server(8082)
        except Exception:
            pass
        print(f"🔌 [Storage] Connecting to NATS: {Config.NATS_URL}")
        try:
            self.nc = await nats.connect(Config.NATS_URL)
            self.js = self.nc.jetstream()
            try:
                metrics.set_health(nats_ok=True)
            except Exception:
                pass
            async def _buf_reporter():
                while True:
                    try:
                        metrics.storage_buffer_depth.set(len(self._buffer))
                    except Exception:
                        pass
                    await asyncio.sleep(2)
            asyncio.create_task(_buf_reporter())
            print("🚀 Storage Worker started, listening to 'asr.output.archive'...")

            await self.js.subscribe(
                "asr.output.archive",
                queue="storage_workers",
                cb=self.process_msg,
                manual_ack=True,
                durable="storage-durable",
            )
            await self.js.subscribe(
                "asr.output",
                queue="storage_workers",
                cb=self.process_msg,
                manual_ack=True,
                durable="storage-durable-compat",
            )
            asyncio.create_task(self._periodic_flush())
            await asyncio.Future()

        except Exception as e:
            logger.critical(f"❌ Startup failed: {e}", exc_info=True)
        finally:
            try:
                await self._flush_buffer()
            except Exception:
                pass
            if self.mongo_async_client:
                self.mongo_async_client.close()
            if self.nc:
                await self.nc.close()


if __name__ == "__main__":
    worker = StorageWorker()
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        logger.info("🛑 Storage Worker stopped.")
