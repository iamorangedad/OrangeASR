import os


class Config:
    # --- Gateway Service (server listening configuration) ---
    API_HOST = "0.0.0.0"
    API_PORT = 8000  # container internal port

    # --- Client Connection (frontend connection configuration) ---
    # Corresponds to the NodePort (30081) defined in K8s YAML
    # If running Gateway locally without K8s, change back to 8000
    WS_URL = os.getenv("WS_URL", "ws://10.0.0.27:30081/ws/realtime")

    # --- NATS Connection (message bus) ---
    # 10.0.0.27 is your host machine / Master node IP
    NATS_URL = os.getenv("NATS_URL", "nats://10.0.0.27:30742")

    # --- NATS Subjects (critical change: read/write separation) ---
    # 1. Input stream: Gateway -> ASR Worker (sends audio chunks)
    SUBJECT_INPUT = "asr.input"

    # 2. Output stream: ASR Worker -> Gateway & Storage (sends recognition results)
    SUBJECT_OUTPUT = "asr.output"

    # 3. Archive stream: target that Storage Worker listens to (now changed to listen to output stream)
    LOG_SUBJECT = SUBJECT_OUTPUT

    # --- MongoDB (metadata storage) ---
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://10.0.0.27:30327")
    MONGO_DB = "asr_data"

    # --- MinIO/S3 (audio file storage) ---
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://10.0.0.27:30091")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password123")
    S3_BUCKET = "audio"

    # --- Refiner Worker (LLM-based ASR correction) ---
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://10.0.0.55:11434")
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:4b")
    REFINE_BATCH_SIZE = int(os.getenv("REFINE_BATCH_SIZE", "3"))
    REFINE_BUFFER_TTL = int(os.getenv("REFINE_BUFFER_TTL", "10"))
    REFINE_TIMEOUT = int(os.getenv("REFINE_TIMEOUT", "30"))
