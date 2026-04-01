import os


class Config:
    # --- Gateway Service (服务端监听配置) ---
    API_HOST = "0.0.0.0"
    API_PORT = 8000  # 容器内部端口

    # --- Client Connection (前端连接配置) ---
    # 对应 K8s YAML 中定义的 NodePort (30081)
    # 如果你是本地运行 Gateway 且没用 K8s，可以改回 8000
    WS_URL = os.getenv("WS_URL", "ws://10.0.0.27:30081/ws/realtime")

    # --- NATS Connection (消息总线) ---
    # 10.0.0.27 是你的宿主机/Master节点 IP
    NATS_URL = os.getenv("NATS_URL", "nats://10.0.0.27:30742")

    # --- NATS Subjects (关键修改：读写分离) ---
    # 1. 输入流: Gateway -> ASR Worker (发送音频片段)
    SUBJECT_INPUT = "asr.input"

    # 2. 输出流: ASR Worker -> Gateway & Storage (发送识别结果)
    SUBJECT_OUTPUT = "asr.output"

    # 3. 归档流: Storage Worker 监听的目标 (现在改为监听输出流)
    LOG_SUBJECT = SUBJECT_OUTPUT

    # --- MongoDB (元数据存储) ---
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://10.0.0.27:30327")
    MONGO_DB = "asr_data"

    # --- MinIO/S3 (音频文件存储) ---
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
