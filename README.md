# OrangeASR

A distributed real-time Automatic Speech Recognition (ASR) system built on a microservices architecture, designed to run on edge devices such as NVIDIA Jetson and Raspberry Pi clusters orchestrated by Kubernetes.

## Architecture

OrangeASR decouples audio ingestion, inference, storage, and the user interface into independent services that communicate via **NATS JetStream** as the message bus.

```
┌─────────────┐      Audio Chunks (base64)      ┌──────────────┐
│  Web UI     │ ──────────────────────────────► │   Gateway    │
│ (Gradio)    │ ◄── Transcription + Latency ─── │ (FastAPI/WS) │
└─────────────┘                                 └──────┬───────┘
                                                       │
                                          NATS JetStream
                                          asr.input
                                                       │
                                                       ▼
                                               ┌───────────────┐
                                               │  ASR Worker   │
                                               │(faster-whisper│
                                               │   CUDA/GPU)   │
                                               └───────┬───────┘
                                                       │
                                          NATS JetStream
                                          asr.output
                                               ┌────┬──┴────┬────────┐
                                               ▼    ▼       ▼        ▼
                                          ┌──────────┐ ┌──────────┐ ┌───────────┐
                                          │ Gateway  │ │ Storage  │ │  Refiner  │
                                          │(response)│ │ Worker   │ │  Worker   │
                                          └──────────┘ └────┬─────┘ └─────┬─────┘
                                                     ┌───────┴───────┐     │
                                                     ▼               ▼     ▼
                                              ┌───────────┐  ┌──────────┐ ┌──────────┐
                                              │  MinIO/S3 │  │ MongoDB  │ │ MongoDB  │
                                              │ (Audio)   │  │(Metadata)│ │(refined) │
                                              └───────────┘  └──────────┘ └──────────┘
```

## Components

| Service | Description | Tech Stack |
|---------|-------------|------------|
| **Gateway** | WebSocket endpoint that receives streaming audio from clients, buffers chunks, and publishes them to NATS. Routes transcription results back to clients. | FastAPI, uvicorn, nats-py |
| **ASR Worker** | Consumes audio chunks from NATS, runs GPU-accelerated Whisper inference, and publishes results. | faster-whisper, PyTorch (CUDA) |
| **Storage Worker** | Archives audio as WAV files to MinIO/S3 and stores transcription metadata in MongoDB. | boto3, pymongo, nats-py |
| **Refiner Worker** | Buffers transcription segments by session, calls Ollama LLM to detect self-corrections, and stores `(original_text, corrected_text)` pairs for fine-tuning. | requests, pymongo, nats-py |
| **Web UI** | Real-time microphone streaming client with live transcription display and latency metrics. | Gradio, websocket-client, scipy |

## Infrastructure

| Component | Purpose | Port (NodePort) |
|-----------|---------|-----------------|
| **NATS JetStream** | Message bus for inter-service communication | 30742 |
| **MongoDB** | Transcription metadata storage | 30327 |
| **MinIO/S3** | Audio file object storage | 30091 |

## Prerequisites

- **Kubernetes cluster** with at least two nodes (e.g., Raspberry Pi + NVIDIA Jetson)
- **NVIDIA Jetson** (Orin Nano or similar, compute capability sm_87) for GPU-accelerated inference
- **kubectl** configured to access your cluster
- **Docker** for building container images

## Quick Start

### 1. Deploy Infrastructure

```bash
kubectl apply -f deploy/k8s/01-infra.yaml
```

This deploys NATS JetStream and MongoDB into the `asr-service` namespace.

### 2. Deploy Application Services

```bash
kubectl apply -f deploy/k8s/02-apps.yaml
```

This deploys the Gateway, ASR Worker, Storage Worker, and Web UI.

### 3. Access the Web UI

Open your browser to:

```
http://<your-node-ip>:30082
```

### 4. Local Development (without K8s)

```bash
pip install -r deploy/requirements.txt

# Start the Gateway
python -m src.gateway

# Start the ASR Worker
python -m src.asr_worker

# Start the Storage Worker
python -m src.storage_worker

# Start the Refiner Worker (requires Ollama server)
python -m src.refiner_worker

# Start the Web UI
python -m src.web_ui
```

## Configuration

All services are configured via environment variables or the `src/config.py` module:

| Variable | Default | Description |
|----------|---------|-------------|
| `WS_URL` | `ws://10.0.0.27:30081/ws/realtime` | WebSocket endpoint for clients |
| `NATS_URL` | `nats://10.0.0.27:30742` | NATS server URL |
| `MONGO_URI` | `mongodb://10.0.0.27:30327` | MongoDB connection string |
| `MONGO_DB` | `asr_data` | MongoDB database name |
| `S3_ENDPOINT` | `http://10.0.0.27:30091` | MinIO/S3 endpoint |
| `S3_ACCESS_KEY` | `admin` | S3 access key |
| `S3_SECRET_KEY` | `password123` | S3 secret key |
| `S3_BUCKET` | `audio` | S3 bucket name |
| `ASR_DEVICE` | `cuda` | Device for Whisper inference (`cuda` or `cpu`) |
| `ASR_COMPUTE_TYPE` | `float16` | Compute precision for inference |
| `OLLAMA_BASE_URL` | `http://10.0.0.55:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `qwen3:4b` | LLM model for ASR correction |
| `REFINE_BATCH_SIZE` | `3` | Min segments per session before refinement |
| `REFINE_BUFFER_TTL` | `10` | Seconds of silence before triggering refinement |
| `REFINE_TIMEOUT` | `30` | Timeout for Ollama API calls (seconds) |

## NATS Subjects

| Subject | Direction | Description |
|---------|-----------|-------------|
| `asr.input` | Gateway → ASR Worker | Base64-encoded audio chunks |
| `asr.output` | ASR Worker → Gateway, Storage & Refiner | Transcription results with metadata |

## Project Structure

```
OrangeASR/
├── src/
│   ├── __init__.py
│   ├── config.py           # Shared configuration
│   ├── gateway.py          # WebSocket + NATS gateway
│   ├── asr_worker.py       # Whisper inference worker
│   ├── storage_worker.py   # MinIO + MongoDB archiver
│   ├── refiner_worker.py   # LLM-based ASR correction worker
│   ├── refiner.py          # Ollama LLM client for refinement
│   ├── web_ui.py           # Gradio streaming UI
│   └── logger.py           # JSON formatter for Loki/Grafana
├── deploy/
│   ├── Dockerfile          # Base container image
│   ├── requirements.txt    # Python dependencies
│   └── k8s/
│       ├── 01-infra.yaml   # NATS + MongoDB deployment
│       ├── 02-apps.yaml    # Application services deployment
│       └── loki-retention.yaml
├── tests/
│   └── src/
│       ├── test_transcribe.py
│       ├── test_refiner.py
│       ├── test_nats_core.py
│       ├── test_mongo_connectivity.py
│       └── test_minio_s3.py
├── assets/
│   └── architecture.png
└── setup.py                # CUDA extension build (Jetson)
```

## Observability

Logs are emitted in JSON format, compatible with **Loki** and **Grafana** for centralized log aggregation. Each log entry includes:

- `service_name` — originating service
- `session_id` — client session identifier
- `req_id` — per-request unique identifier
- `latency` — inference latency in seconds

## License

This project is provided as-is for educational and development purposes.
