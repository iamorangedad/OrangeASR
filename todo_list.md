# OrangeASR Development Roadmap & Optimization Plan

## Overview

This document outlines the system architecture optimizations and development roadmap for the **OrangeASR** project. The target is to transition the current prototype from a developer-focused, hardcoded setup into a cloud-native, resilient, and production-ready distributed ASR system.

---

## 1. Service Discovery & Networking Optimization

### Current Issue

All microservice connections in `02-apps.yaml` rely on hardcoded physical host IPs (e.g., `10.0.0.27:30742`) within the `asr-config` ConfigMap. This tightly couples services to specific infrastructure, making node failures or IP changes fatal to system availability.

### Tasks for Agent

* **Switch to K8s Internal DNS:** Replace all physical IP addresses in `asr-config` with cross-namespace K8s DNS names:


* **NATS:** `nats.asr-service.svc.cluster.local:4222`
* **MongoDB:** `mongo.asr-service.svc.cluster.local:27017`
* **MinIO:** `minio-service.asr-service.svc.cluster.local:9000`


* **Internalize Infrastructure Services:** Update infrastructure services in `01-infra.yaml` from `NodePort` to `ClusterIP` to restrict internal traffic within the K8s cluster and reduce unnecessary host port exposures.



---

## 2. Production Security & Image Delivery Pipeline

### Current Issue

Services use `hostPath` mounts (e.g., `/home/mac/OrangeASR`) to inject code at runtime. Additionally, sensitive credentials (e.g., `S3_SECRET_KEY`) are exposed as plain text in ConfigMaps.

### Tasks for Agent

* **Standardize Container Images:** Remove all `hostPath` volume mounts from Deployment manifests. Update Dockerfiles to copy source code into the images (`COPY . /app`) for version-controlled deployment.


* **Secret Management:** Extract sensitive environment variables (MinIO keys, DB passwords) out of `asr-config` ConfigMap and migrate them to Kubernetes `Secret` objects.



---

## 3. High Availability (HA) & Scheduling Decoupling

### Current Issue

Deployments use explicit `nodeName` bindings (e.g., `raspberrypi` or `ubuntu`), bypassing K8s scheduler logic. Services operate as single replicas (`replicas: 1`), creating single points of failure (SPOF).

### Tasks for Agent

* **Decouple Node Names:** Remove `nodeName` properties from all Deployment specs. Use `nodeSelector` or `nodeAffinity` with node labels (e.g., `kubernetes.io/arch: arm64` or `gpu-node: "true"`).


* **Horizontal Scaling & Consumer Queue Groups:**
* Increase `asr-gateway` replicas to `2+` and configure a `HorizontalPodAutoscaler` (HPA).


* Implement NATS JetStream **Queue Groups** within `asr-worker` and `asr-refiner` codebases to support multi-worker concurrent stream processing across multiple GPU/CPU nodes.





---

## 4. Resource Allocation & Inference Acceleration

### Current Issue

The `asr-worker` claims an entire dedicated GPU (`nvidia.com/gpu: 1`), which leads to low resource utilization when running lightweight Whisper models.

### Tasks for Agent

* **GPU Sharing Strategy:** Evaluate NVIDIA Time-Slicing or MPS (Multi-Process Service) in the K8s device plugin setup to allow multiple worker processes or Pods to share a single GPU.
* **Model Inference Tuning:** Ensure `faster-whisper` uses quantized compute types (`int8` or `int8_float16`) to reduce VRAM consumption and boost inference throughput.

---

## 5. Storage Reliability & Persistence

### Current Issue

MinIO and MongoDB rely on local host paths (`/data/minio` and `/data/mongo_data`) on specific nodes. Node failure results in permanent data loss.

### Tasks for Agent

* Integrate a lightweight distributed storage provisioner (e.g., **Longhorn** or K3s **Local Path Provisioner**) to replace direct `hostPath` volume mounts with dynamic PVC provisioning for MinIO and MongoDB.



---

## 6. Health Checks & Observability

### Current Issue

Deployments currently lack `livenessProbe` and `readinessProbe` definitions. Unhealthy containers or disconnected background workers cannot be auto-healed by Kubernetes.

### Tasks for Agent

* **Add Health Probes:** Configure `readinessProbe` and `livenessProbe` endpoints for HTTP services (`asr-gateway` and `asr-web-ui`).


* **Worker Heartbeats:** Implement health check mechanisms for background worker processes (`asr-worker`, `asr-refiner`, `asr-storage`).


* **Metrics Monitoring:** Expose Prometheus metrics for NATS queue lag, worker processing latency, and GPU utilization.

---

## 7. Performance Bottleneck & End-to-End Data Pipeline Optimization ⭐ NEW

### 7.1 Bottleneck Analysis (Frontend → Gateway → NATS → Model → Storage)

#### A. Web UI Frontend (`src/web_ui.py`)

| Issue | Location | Impact |
|-------|----------|--------|
| **Sync WebSocket in async UI** — `websocket.create_connection` (sync `websocket-client`) blocks Gradio event loop; `send_audio_chunk` does blocking `ws.send_binary` | `web_ui.py:60,100` | UI freezes under load, no backpressure |
| **Per-chunk heavy resampling** — `resample_poly` + `np.gcd` + `validate_and_normalize` executed on main thread per Gradio `stream` event | `web_ui.py:83-99` | CPU 20-40ms/chunk, repeated filter recomputation |
| **No send throttling / debouncing** — Gradio fires ~10-20Hz, but Gateway buffers 2s blindly | `web_ui.py:199` | Redundant small packets, wasted encode |
| **Polling receive queue only on send** — `recv_queue.get_nowait` only inside `process_stream` | `web_ui.py:147` | Display latency = audio interval, not real ASR latency |

#### B. Gateway (`src/gateway.py`)

| Issue | Location | Impact |
|-------|----------|--------|
| **Fixed 2.0s hard cut** — `THRESHOLD_BYTES = BYTES_PER_SEC * 2.0` with `audio_buffer.clear()` | `gateway.py:119,148` | 2s + inference E2E latency, word truncation at boundaries, higher WER |
| **Base64 33% overhead** — `base64.b64encode(audio_buffer).decode()` per chunk, JSON-encoded | `gateway.py:131` | +33% NATS bandwidth, +CPU encode/decode, hits `max_payload 10MB` faster |
| **No overlap / VAD** — arbitrary byte cut, no voice activity detection | `gateway.py:124` | Truncated words, wasted inference on silence |
| **Single durable without queue group** — `"gateway_router"` on `asr.output` | `gateway.py:97` | Cannot scale gateway to >1 replica, message loss on rebalance |
| **No backpressure / rate limit** — `receive_bytes` infinite loop, unbounded `js.publish` | `gateway.py:122` | NATS pending queue explosion under flood |
| **No JetStream publish ack handling** | `gateway.py:136` | Silent message loss |

#### C. NATS JetStream (`deploy/k8s/01-infra.yaml` + workers)

| Issue | Location | Impact |
|-------|----------|--------|
| **Ad-hoc stream creation race** — each worker `add_stream` with same subjects `asr.input`/`asr.output` but different names `ASR_INPUT`/`ASR_ARCHIVE`/`ASR_REFINER` | `asr_worker.py:124`, `storage_worker.py:158`, `refiner_worker.py:149` | Stream creation conflict, unpredictable retention |
| **No stream tuning** — no `max_age`, `max_bytes`, `retention`, `ack_policy`, `max_msgs` | all workers | Memory bloat, no TTL, `asr.output` re-publishes full `audio_b64` doubling bandwidth |
| **Redundant audio echo** — `asr_worker` republishes `audio_b64` in `asr.output` for storage, but refiner/gateway don't need it | `asr_worker.py:105` | 2x bandwidth waste; refiner receives heavy payload unnecessarily |

#### D. ASR Worker (`src/asr_worker.py`)

| Issue | Location | Impact |
|-------|----------|--------|
| **Base64 decode per msg + float conversion** | `asr_worker.py:79-81` | CPU overhead |
| **Unbounded thread pool** — `run_in_executor(None, ...)` default pool | `asr_worker.py:85` | Thread explosion under burst |
| **No batching / no streaming decode** — 1 chunk = 1 inference, `beam_size=1` tiny model underutilizes GPU | `asr_worker.py:50` | GPU util <20%, throughput bottleneck |
| **No model warmup, no KV cache** | `asr_worker.py:28` | First inference cold start ~1-2s |
| **`float16` not quantized** — should be `int8_float16` on Orin Nano sm_87 | `asr_worker.py:14` | 2x VRAM, ~1.5x latency vs int8 |
| **Blocking inference without timeout** | `asr_worker.py:85` | One slow chunk blocks worker |

#### E. Storage Worker (`src/storage_worker.py`)

| Issue | Location | Impact |
|-------|----------|--------|
| **Sync blocking I/O in async loop** — `boto3.upload_fileobj` + `pymongo.insert_one` block event loop | `storage_worker.py:102,126` | Throughput collapse, `process_msg` latency spikes |
| **Per-msg DB insert, no bulk** | `storage_worker.py:126` | MongoDB pressure, 1 RTT/msg |
| **WAV header alloc per msg** — new `BytesIO` + `wave.open` | `storage_worker.py:61` | GC pressure |

#### F. Refiner Worker (`src/refiner_worker.py` + `src/refiner.py`)

| Issue | Location | Impact |
|-------|----------|--------|
| **Sync `requests.post` inside async** — blocks loop 30s `REFINE_TIMEOUT` | `refiner.py:56` | Starves all sessions, timer drift |
| **`call_later` + `ensure_future` leak** | `refiner_worker.py:85` | Timer not cancelled on shutdown correctly |
| **No retry / circuit breaker for Ollama** | `refiner.py:56` | Single LLM failure drops session |
| **Prompt rebuilt per call with `json.dumps`** | `refiner.py:43` | Minor CPU waste |

### 7.2 Optimization Tasks

#### P0 - Critical Path (E2E Latency -50%)

* **Gateway: Replace fixed 2s buffer with VAD-driven chunking:**
  * Integrate `webrtcvad` or reuse `faster-whisper` VAD parameters server-side; use sliding window with 0.5s overlap to avoid word truncation
  * Make `THRESHOLD_BYTES` adaptive: 0.8-1.5s + silence detection (`min_silence_duration_ms=300`); fallback to max 2s
  * Keep overlap buffer (e.g., last 0.3s) instead of `clear()` → `audio_buffer = audio_buffer[-overlap:]`

* **Eliminate Base64 — use binary NATS payload:**
  * Publish `audio` as raw bytes via NATS `msg.data` + JSON header with `msg.headers` (or `msgpack`/`protobuf`); alternatively keep JSON but compress with `zlib`/`lz4`
  * Expected saving: -33% bandwidth, -5ms encode/decode per hop
  * Update all workers to handle `payload["audio"]` as bytes; keep backward compat flag

* **ASR Worker: Quantization + Batching:**
  * Change `ASR_COMPUTE_TYPE` default to `int8_float16` (validate on sm_87) in `config.py:14` and `02-apps.yaml`
  * Implement micro-batching: queue incoming `asr.input` msgs for 50-100ms, run batched `model.transcribe` (or concurrent `run_in_executor` with semaphore `max_concurrency=2`)
  * Add model warmup on startup: dummy inference 1s silence

* **Gateway: Async NATS publish with ack + backpressure:**
  * Check `await js.publish(..., timeout=2)` ack; on `slow consumer` apply token bucket rate limit (e.g., 10 msgs/sec/session)
  * Add `max_pending` per WebSocket, drop oldest or send `busy` signal to client

#### P1 - Throughput & Resource Efficiency

* **Web UI: Async WebSocket + resampling optimization:**
  * Replace `websocket-client` sync with `websockets` async or run sync client in dedicated thread with `queue.Queue` for send
  * Cache `resample_poly` filter: precompute `up/down` + `fir` coefficients per `sr` pair; use `scipy.signal.resample_poly` with cached `window`
  * Alternatively use `librosa.resample` or `soxr` for faster path; move `validate_and_normalize` to `numba` or vectorized path
  * Decouple recv poll: separate thread polls `recv_queue` and triggers `gr.update` via `demo.queue` callback, not tied to `process_stream` invocation

* **Storage Worker: Async I/O:**
  * Replace `boto3` → `aioboto3` + `motor` async Mongo; wrap `process_msg` `upload_fileobj`/`insert_one` in `run_in_executor` if sync kept
  * Implement bulk insert: buffer 10 msgs / 1s then `insert_many`; reuse `BytesIO` pool
  * Add S3 multipart threshold for large WAV

* **Refiner Worker: Async LLM client:**
  * Replace `requests` → `httpx.AsyncClient` with `timeout=30`, retry 2x with exponential backoff, circuit breaker after 3 failures
  * Run `refine()` via `run_in_executor` or native async; make `_refine_session` fully async
  * Fix timer leak: store `handle.cancel()` correctly in `pop` branch, guard `loop.call_later` with `try/except RuntimeError`

* **NATS: Unified stream definition:**
  * Create single init job (`deploy/k8s/nats-init.yaml`) that declares streams `ASR_INPUT` (`asr.input`) and `ASR_OUTPUT` (`asr.output`) with `retention=limits`, `max_age=5m`, `max_bytes=512MB`, `storage=file`
  * Workers use `js.subscribe(..., durable=..., deliver_policy=...)` only, remove `add_stream` try/except
  * Split `asr.output` into `asr.output.transcript` (light, for gateway/refiner) and `asr.output.archive` (with `audio_b64` only for storage) OR use NATS headers to filter

#### P2 - Scalability & Observability

* **Metrics:**
  * Add Prometheus counters: `asr_gateway_ws_connections`, `asr_nats_publish_latency`, `asr_worker_inference_seconds`, `asr_worker_queue_depth`, `storage_s3_upload_seconds`, `refiner_llm_latency`
  * Expose `/metrics` on gateway (`prometheus_client`) and workers via sidecar HTTP server
  * E2E latency: embed `client_capture_ts` in payload, compute `display_ts - capture_ts` in web_ui

* **Health & Autoscaling:**
  * Gateway: add `livenessProbe` `/healthz` + `readinessProbe` checking NATS connectivity
  * Workers: heartbeat file or HTTP `/healthz` that checks model loaded + NATS subscribed
  * HPA on `asr-gateway` CPU 70% + custom metric `nats_consumer_pending`

* **Backpressure protocol:**
  * Gateway → WebUI: send `{"type":"backpressure","pending":N}` when NATS pending > threshold; WebUI throttles `send_audio_chunk` (drop or coalesce)

#### P3 - Workflow Redesign (Optional Future)

* **Evaluate streaming ASR:** Replace 2s chunk + `faster-whisper` batch with streaming model (`whisper-streaming`, `webrtcvad` + `parakeet` or `nemo` streaming) for <300ms latency
* **Consider gRPC streaming** as alternative to WebSocket+Base64+NATS for audio ingress (higher throughput, binary framing)
* **Audio compression:** Opus encode on frontend (20kbps) → decode in worker, reduces bandwidth 8x vs PCM16

### 7.3 Expected Gains

| Metric | Before | After (P0) | After (P0+P1) |
|--------|--------|------------|---------------|
| E2E latency (2s chunk) | 2.5-3.5s | 1.0-1.5s | 0.8-1.2s |
| NATS bandwidth | 85KB/msg | 55KB/msg (-35%) | 15KB/msg with Opus (-82%) |
| ASR worker throughput | 0.5 req/s/GPU | 1.5 req/s (int8+batch) | 3 req/s (batch+overlap) |
| Storage worker throughput | 2 msg/s (blocking) | 10 msg/s (async bulk) | 20 msg/s |
| Gateway replicas | 1 (SPOF) | 2+ with queue group | HPA 2-5 |

### 7.4 Verification Plan

* Benchmark: `tests/src/test_transcribe.py` + new `tests/benchmark_e2e.py` measuring `capture→display` latency with 10 concurrent clients
* Load test: `pytest tests/src/test_nats_core.py` with 100 msgs/sec flood, assert no `max_payload` error and pending <100
* Profile: `py-spy` on `web_ui.py` resample, `asr_worker.py` inference; `nats` monitoring `/jsz` for pending
