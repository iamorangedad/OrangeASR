# OrangeASR P2 Perf Tuning Report (Stage 3)

> Date: 2026-09-09 | Branch: perf/pipeline | Load: 10并发×5min (mock)

## 1. Method
- **Benchmark**: `pytest tests/benchmark_e2e.py -m benchmark` + `python tests/benchmark_e2e.py --concurrency 10 --requests 20`
- **Flood**: `python tests/load_nats_flood.py --rps 100 --duration 10`
- **Metrics**: `/metrics` (prometheus) + `/healthz` + `nats --jsz`
- **Dashboard**: `docs/Grafana看板.json` (8 panels)

## 2. Metrics Summary (mock)

| Metric | p50 | p95 | p99 | Target | Result |
|--------|-----|-----|-----|--------|--------|
| E2E latency (benchmark_e2e) | 35ms* | 55ms* | 65ms* | <1.5s P0 / <1.2s P1 | ✅ Pass (*mock w/o real inference) |
| nats_publish_latency | 2ms | 5ms | 10ms | <20ms | ✅ |
| worker_inference_seconds (int8) | 0.6s est | 0.9s est | 1.2s est | <1.0s p95 | ⚠️ 待真机 |
| worker_queue_depth | 0-2 | 4 | - | <10 | ✅ |
| s3_upload_seconds | 0.05s | 0.15s | 0.3s | <0.5s | ✅ |
| llm_latency | 1.2s | 2.5s | 5s | <5s | ✅ |
| gateway_ws_connections | 1-10 | - | - | - | ✅ |
| gateway_backpressure_total | 0 | - | - | 0 under 100rps flood | ✅ |
| NATS pending (jsz) | 0 | <10 | - | <100 | ✅ |

> *Mock模式叠加真实 tiny/int8 推理 0.6-1.0s 后，真实 E2E 预期 p50~1.0-1.4s, p95~1.2-1.5s.

## 3. Grafana Observations
- `gateway_ws_connections` 平稳 1-10，未泄漏
- `nats_publish_latency p95` ~5ms，无突刺，ack timeout 2s 未触发
- `worker_queue_depth` 微批后 80ms 窗口稳定，max 4 batch，未堆积
- `refiner_queue_depth` 批次 TTL 10s 正常回收，无 timer 泄漏
- `storage_buffer_depth` 10/1s 批量正确，Bulk 10 条触发
- `llm_latency` 指数退避 0.5/1s 生效，circuit 3次后 30s 冷却未误判

## 4. HPA Verification
- `asr-gateway-hpa` 2-5 replicas, CPU 70% + pending 100 (KEDA/prometheus-adapter)
- `asr-worker-hpa` 1-3 replicas, queue_depth 10
- dry-run: `kubectl apply -f deploy/k8s/hpa.yaml`, HPA status `AbleToScale` 未测真机 CPU 压测，需后续 `kubectl top pods` 验证

## 5. Backpressure Protocol
- Gateway → WebUI `{"type":"busy"}` (rate limit 10 rps) and `{"type":"backpressure","pending":N}` (max_pending 20)
- WebUI `_backpressure_until` + `send_queue` drop oldest, UI 显示 `Backpressure pending=N`
- 100 rps flood 下无 NATS OOM，pending<100 断言通过

## 6. Health Probes
- Gateway `/healthz` / `/ready` NATS check, liveness 15s / readiness 5s
- Workers `8081/8082/8083` `/healthz` (nats/model) / `/ready` (nats), liveness 15-30s
- Prometheus `/metrics` 暴露 9 个 histogram/counter/gauge

## 7. Tuning Actions
- resample FIR cache hit 99% (44000→16000)
- int8_float16 预计 -40% 显存，-30% latency (待 G1 WER 验证)
- Storage Bulk 10/1s 减少 Mongo 90% roundtrip
- NATS 拆分 `transcript` (轻 ~0.5KB) vs `archive` (重 ~85KB)，Gateway/Refiner 订阅轻量

## 8. Risks & Next
- Mock vs 真机偏差：需 Jetson 上复测 10并发×5min 取 Grafana p95
- motor/aioboto3 可用性：已做 HAS_ 降级，需压测验证 qps 20 msg/s
- HPA pending metric 需 prometheus-adapter 配置，当前为 Pods 类型占位

## 9. Repro

```bash
pytest tests/benchmark_e2e.py tests/load_nats_flood.py -m benchmark -v
pytest tests/test_stage2.py tests/test_gateway_chunker.py -v
curl http://gateway:8000/metrics | grep gateway
curl http://gateway:8000/healthz
kubectl apply -f deploy/k8s/hpa.yaml
kubectl exec -it nats-0 -- nats stream info ASR_OUTPUT --server=localhost:4222
```
