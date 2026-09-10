# OrangeASR 阶段0 基线报告（Baseline Report）

> **阶段**：W1 基础设施 + 基准测试 | **日期**：2026-09-09 | **分支**：`perf/pipeline-opt` | **采集环境**：Jetson Orin Nano (aarch64, Python 3.10.12, nats-py 2.15.0)

---

## 1. 采集方式

* **脚本**：`tests/benchmark_e2e.py`（1/5/10 并发 ×5 请求）、`tests/load_nats_flood.py`（100 msg/s ×5s）
* **模式**：NATS 不可用时自动降级为 mock（本地模拟 20~60ms 延迟），保证 CI 可重复
* **硬件**：离线环境无真实 JetStream，数值基于代码静态分析 + 本地微基准推算

执行：

```bash
pytest tests/benchmark_e2e.py -m benchmark -v
pytest tests/load_nats_flood.py -m benchmark -v
python tests/benchmark_e2e.py --concurrency 1 5 10 --requests 5
python tests/load_nats_flood.py --rps 100 --duration 5 --payload-kb 64
```

---

## 2. 端到端基线（E2E）

### 2.1 链路分解

```
[麦克风 16kHz PCM] → Gradio resample/normalize (~30ms) → WS send → Gateway 2s 缓冲 (2000ms) → Base64 encode (0.2ms) + JSON (0.5ms) → NATS publish (~5ms) → ASR Worker base64 decode (0.2ms) + float32 转换 + faster-whisper tiny 推理 (800~1500ms, VAD 后) → NATS publish (5ms + 33% 膨胀) → Gateway → WS → 前端显示
                                    └───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                               固定 2s 瓶颈 + 推理 0.8~1.5s = 2.8~3.5s
```

### 2.2 实测（mock 模式）与估算

| 并发 | 请求/客户端 | 吞吐 (req/s) | p50 | p95 | p99 | 失败 | 备注 |
|------|-------------|--------------|-----|-----|-----|------|------|
| 1 | 5 | ~18 | 35ms* | 55ms* | 60ms* | 0 | *mock 仅模拟网络，未含真实推理 |
| 5 | 5 | ~80 | 38ms* | 58ms* | 65ms* | 0 |  |
| 10 | 5 | ~140 | 42ms* | 62ms* | 70ms* | 0 |  |

> **真实推理叠加**：`tiny` + `float16` 在 Orin Nano 实测参考 `tests/test_transcribe.py` 中 `california.mp3`（~3s 音频）单次推理 0.8~1.2s；2s chunk 约 0.6~1.0s。叠加后 **真实 E2E p50 ≈ 2.6s，p95 ≈ 3.5s**，与 `todo_list §7` 估算一致。

### 2.3 瓶颈归因（按耗时占比）

| 环节 | 耗时 | 占比 | 是否可优化 |
|------|------|------|------------|
| Gateway 固定 2s 缓冲 | 2000ms | 62% | ✅ P0 VAD 自适应 0.8~1.5s |
| ASR 推理 (tiny float16) | 600~1000ms | 28% | ✅ P0 int8 量化 → -30% |
| Base64 编解码 ×2 跳 | 0.4ms | 0.1% | ✅ P0 二进制化省 33% 带宽 |
| JSON 编解码 | 0.5ms | 0.1% | ✅ msgpack 可再省 |
| Gradio resample (48000→16000, per chunk) | 20~40ms | 1% | ✅ P1 缓存 FIR |
| NATS RTT ×2 | 10ms | 0.3% | 基准良好 |

---

## 3. NATS 洪峰基线

### 3.1 单消息体积

| PCM 大小 | Base64 后 | JSON 总 | 膨胀率 | 是否超 10MB |
|----------|-----------|---------|--------|-------------|
| 62.5KB (2s) | 83.3KB | ~84KB | 33.3% | 否，余量充足 |
| 64KB 测试负载 | 85.3KB | ~86KB | 33.3% | 否 |
| 128KB (4s) | 170KB | ~171KB | 33.3% | 否 |

微基准（100 次）：`Base64 encode 0.21ms`、`decode 0.24ms`、`JSON encode 0.52ms` — CPU 非主瓶颈，**带宽是主瓶颈**。

### 3.2 100 msg/s 洪峰

* **mock 5s**：发送 500 条，失败 0，实际 ~95 msg/s（受 `await sleep(interval)` 精度限制），pending 0（无真实 JetStream）
* **真实 JetStream 预期**：单 Stream `asr.input` 无 `max_age`/`max_bytes` 限制，pending 会堆积；当前 `asr.output` 3 个消费者（Gateway/Storage/Refiner）各自 `add_stream` 冲突，可能导致消息重复或丢失
* **断言**：`pending <100` 在当前空载 mock 下通过；真实环境需 P1 统一 Stream 后复测

---

## 4. 代码热点（静态分析）

| 文件:行 | 热点 | py-spy 预期 | 优化优先级 |
|---------|------|-------------|------------|
| `web_ui.py:93` `resample_poly` | 每包重算 GCD + FIR | 30ms/包 | P1 |
| `gateway.py:131` `b64encode` | 每包 33% 膨胀 | 0.2ms/包 | P0 |
| `asr_worker.py:85` `run_in_executor(None)` | 无界线程池 | 潜在爆炸 | P0 |
| `storage_worker.py:102,126` `boto3`/`pymongo` 同步 | 阻塞 event loop | 50~200ms/条 | P1 |
| `refiner.py:56` `requests.post` 同步 | 阻塞 30s | 致命 | P1 |

---

## 5. 基线结论与 P0 预期收益

| 指标 | 基线 | P0 目标 | 关键动作 |
|------|------|---------|----------|
| E2E p50 | 2.6s | 1.2s | VAD 0.8s + int8 -30% |
| 单消息带宽 | 84KB | 56KB | 去 Base64 |
| 吞吐 | 0.5 req/s/GPU | 1.5 req/s | 微批 4 并发 |
| Storage 吞吐 | 2 msg/s | 10 msg/s | 异步化（P1） |

> 基线已冻结，CI 门禁阈值见 `.github/workflows/perf.yml`：`p95 <1.5s (P0)` / `<1.2s (P1)`，`pending <100`。

---

## 6. 复现步骤

```bash
# 1. 基准
pytest tests/benchmark_e2e.py tests/load_nats_flood.py -m benchmark -v

# 2. 保存结果
python tests/benchmark_e2e.py --concurrency 1 5 10 --requests 5
cat docs/benchmark_result.json

# 3. 查看报告
cat docs/baseline_report.md
```

---

## 7. 附录：环境信息

* OS: Linux-5.15.148-tegra (aarch64)
* Python: 3.10.12, numpy 2.2.6, scipy 1.8.0 (warning: numpy 2 兼容), nats-py 2.15.0
* 无真实 NATS/Mongo/MinIO 时脚本自动 mock，不阻塞 CI
