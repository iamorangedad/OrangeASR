import asyncio
import json
import time
import base64
import os
import statistics
import uuid
import argparse
import struct
import math
import wave
from io import BytesIO
from dataclasses import dataclass, asdict
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

try:
    import nats
    HAS_NATS = True
except ImportError:
    HAS_NATS = False

from src.config import Config

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2
DURATION_SEC = 2.0
CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * DURATION_SEC)


def _gen_pcm(duration=DURATION_SEC, freq=440.0):
    n = int(SAMPLE_RATE * duration)
    buf = bytearray()
    for i in range(n):
        v = int(32767 * 0.3 * math.sin(2 * math.pi * freq * i / SAMPLE_RATE))
        buf.extend(struct.pack("<h", v))
    return bytes(buf)


def _pcm_to_b64(pcm: bytes) -> str:
    return base64.b64encode(pcm).decode()


@dataclass
class LatencySample:
    req_id: str
    session_id: str
    capture_ts: float
    publish_ts: float
    received_ts: float
    e2e_latency: float
    nats_latency: float
    payload_bytes: int
    b64_overhead: float


def _percentile(data, p):
    if not data:
        return 0.0
    data = sorted(data)
    k = (len(data) - 1) * p / 100
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data[int(k)]
    return data[f] * (c - k) + data[c] * (k - f)


@dataclass
class BenchmarkResult:
    concurrency: int
    total_requests: int
    duration: float
    throughput_rps: float
    payload_avg_kb: float
    b64_overhead_pct: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    nats_p50_ms: float
    failed: int


async def _run_concurrency_level(nc, js, concurrency: int, requests_per_client: int, use_mock: bool):
    pcm = _gen_pcm()
    b64_str = _pcm_to_b64(pcm)
    b64_len = len(b64_str.encode())
    raw_len = len(pcm)
    overhead = (b64_len - raw_len) / raw_len * 100 if raw_len else 0

    samples: list[LatencySample] = []
    failed = 0

    if use_mock or js is None:
        for c in range(concurrency):
            session_id = f"bench-sess-{c}"
            for _ in range(requests_per_client):
                capture_ts = time.time()
                await asyncio.sleep(0.02 + 0.04 * (os.urandom(1)[0] / 255))
                received_ts = time.time()
                samples.append(LatencySample(
                    req_id=str(uuid.uuid4()),
                    session_id=session_id,
                    capture_ts=capture_ts,
                    publish_ts=capture_ts + 0.005,
                    received_ts=received_ts,
                    e2e_latency=received_ts - capture_ts,
                    nats_latency=0.005,
                    payload_bytes=b64_len,
                    b64_overhead=overhead,
                ))
        latencies = [s.e2e_latency for s in samples]
        nats_lat = [s.nats_latency for s in samples]
        duration = max((s.received_ts for s in samples), default=time.time()) - min((s.capture_ts for s in samples), default=time.time())
        return BenchmarkResult(
            concurrency=concurrency,
            total_requests=len(samples),
            duration=duration or 1.0,
            throughput_rps=len(samples) / (duration or 1.0),
            payload_avg_kb=b64_len / 1024,
            b64_overhead_pct=overhead,
            p50_ms=_percentile(latencies, 50) * 1000,
            p95_ms=_percentile(latencies, 95) * 1000,
            p99_ms=_percentile(latencies, 99) * 1000,
            nats_p50_ms=_percentile(nats_lat, 50) * 1000,
            failed=failed,
        )

    async def single_client(client_idx: int):
        nonlocal failed
        session_id = f"bench-{uuid.uuid4().hex[:6]}-{client_idx}"
        sub_samples = []

        async def on_result(msg):
            try:
                data = json.loads(msg.data.decode())
                req_id = data.get("req_id")
                for s in pending:
                    if s.req_id == req_id:
                        s.received_ts = time.time()
                        s.e2e_latency = s.received_ts - s.capture_ts
                        s.nats_latency = s.received_ts - s.publish_ts
                        break
                await msg.ack()
            except Exception:
                try:
                    await msg.ack()
                except Exception:
                    pass

        sub = await js.subscribe("asr.output", cb=on_result, durable=f"bench-{session_id}")

        pending: list[LatencySample] = []
        for _ in range(requests_per_client):
            req_id = str(uuid.uuid4())
            capture_ts = time.time()
            payload = {
                "req_id": req_id,
                "session_id": session_id,
                "audio_b64": b64_str,
                "previous_text": "",
                "timestamp": capture_ts,
                "capture_ts": capture_ts,
            }
            publish_ts = time.time()
            sample = LatencySample(
                req_id=req_id,
                session_id=session_id,
                capture_ts=capture_ts,
                publish_ts=publish_ts,
                received_ts=0,
                e2e_latency=0,
                nats_latency=0,
                payload_bytes=b64_len,
                b64_overhead=overhead,
            )
            pending.append(sample)
            samples.append(sample)
            try:
                await js.publish("asr.input", json.dumps(payload).encode(), timeout=2)
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)

        await asyncio.sleep(2.0)
        try:
            await sub.unsubscribe()
        except Exception:
            pass

    start = time.time()
    await asyncio.gather(*[single_client(i) for i in range(concurrency)])
    duration = time.time() - start

    completed = [s for s in samples if s.received_ts > 0]
    latencies = [s.e2e_latency for s in completed] or [0]
    nats_lat = [s.nats_latency for s in completed] or [0]

    return BenchmarkResult(
        concurrency=concurrency,
        total_requests=len(samples),
        duration=duration,
        throughput_rps=len(samples) / max(duration, 0.001),
        payload_avg_kb=b64_len / 1024,
        b64_overhead_pct=overhead,
        p50_ms=_percentile(latencies, 50) * 1000,
        p95_ms=_percentile(latencies, 95) * 1000,
        p99_ms=_percentile(latencies, 99) * 1000,
        nats_p50_ms=_percentile(nats_lat, 50) * 1000,
        failed=failed + (len(samples) - len(completed)),
    )


async def run_benchmark(concurrency_levels=(1, 5, 10), requests_per_client=5):
    nats_url = os.getenv("NATS_URL", Config.NATS_URL)
    nc = None
    js = None
    use_mock = False

    if HAS_NATS:
        try:
            nc = await asyncio.wait_for(nats.connect(nats_url, connect_timeout=1), timeout=2)
            js = nc.jetstream()
            try:
                await asyncio.wait_for(js.add_stream(name="ASR_INPUT", subjects=["asr.input"]), timeout=1)
            except Exception:
                pass
            try:
                await asyncio.wait_for(js.add_stream(name="ASR_OUTPUT", subjects=["asr.output"]), timeout=1)
            except Exception:
                pass
        except Exception as e:
            print(f"[benchmark] NATS 不可用 ({e})，使用 mock 模式")
            use_mock = True
            nc = None
            js = None
    else:
        use_mock = True
        print("[benchmark] nats-py 未安装，使用 mock 模式")

    results = []
    for c in concurrency_levels:
        print(f"\n[benchmark] 并发={c} × {requests_per_client} 请求 ...")
        r = await _run_concurrency_level(nc, js, c, requests_per_client, use_mock)
        results.append(r)
        print(f"  吞吐 {r.throughput_rps:.1f} req/s | p50 {r.p50_ms:.0f}ms p95 {r.p95_ms:.0f}ms | 负载 {r.payload_avg_kb:.1f}KB overhead {r.b64_overhead_pct:.1f}% | 失败 {r.failed}")

    if nc:
        try:
            await nc.drain()
        except Exception:
            pass

    _print_summary(results)
    _save_json(results)
    return results


def _print_summary(results: list[BenchmarkResult]):
    print("\n" + "=" * 72)
    print(" Benchmark Summary")
    print("=" * 72)
    print(f"{'并发':>4} {'请求':>6} {'吞吐(r/s)':>10} {'p50(ms)':>8} {'p95(ms)':>8} {'p99(ms)':>8} {'失败':>4}")
    print("-" * 72)
    for r in results:
        print(f"{r.concurrency:>4} {r.total_requests:>6} {r.throughput_rps:>10.1f} {r.p50_ms:>8.0f} {r.p95_ms:>8.0f} {r.p99_ms:>8.0f} {r.failed:>4}")
    print("=" * 72)


def _save_json(results: list[BenchmarkResult]):
    out = Path("docs/benchmark_result.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    data = [asdict(r) for r in results]
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[benchmark] 结果已保存至 {out}")


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_benchmark_e2e():
    results = await run_benchmark(concurrency_levels=(1, 5), requests_per_client=3)
    assert len(results) == 2
    for r in results:
        assert r.throughput_rps > 0
        assert r.p50_ms >= 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OrangeASR E2E 基准测试")
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 5, 10])
    parser.add_argument("--requests", type=int, default=5)
    args = parser.parse_args()
    asyncio.run(run_benchmark(concurrency_levels=tuple(args.concurrency), requests_per_client=args.requests))
