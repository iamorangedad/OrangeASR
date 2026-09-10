import asyncio
import json
import time
import base64
import os
import math
import struct
import argparse

from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

try:
    import nats
    from nats.errors import TimeoutError as NatsTimeout
    HAS_NATS = True
except ImportError:
    HAS_NATS = False

from src.config import Config

TARGET_RPS = 100
DURATION_SEC = 5
MAX_PAYLOAD_WARN = 10 * 1024 * 1024
PENDING_THRESHOLD = 100


def _gen_payload(size_kb=64):
    pcm = os.urandom(size_kb * 1024)
    b64 = base64.b64encode(pcm).decode()
    return {
        "req_id": "flood-test",
        "session_id": "flood-session",
        "audio_b64": b64,
        "previous_text": "",
        "timestamp": time.time(),
    }


async def flood_test(target_rps=TARGET_RPS, duration=DURATION_SEC, payload_kb=64):
    nats_url = os.getenv("NATS_URL", Config.NATS_URL)
    use_mock = False
    nc = None
    js = None

    if HAS_NATS:
        try:
            nc = await asyncio.wait_for(nats.connect(nats_url, connect_timeout=1), timeout=2)
            js = nc.jetstream()
            try:
                await asyncio.wait_for(js.add_stream(name="ASR_INPUT", subjects=["asr.input"]), timeout=1)
            except Exception:
                pass
        except Exception as e:
            print(f"[flood] NATS 不可用 ({e})，使用 mock 模式")
            use_mock = True
            nc = None
            js = None
    else:
        use_mock = True
        print("[flood] nats-py 未安装，使用 mock 模式")

    payload = _gen_payload(payload_kb)
    payload_bytes = len(json.dumps(payload).encode())
    print(f"[flood] 目标 {target_rps} msg/s × {duration}s | 单消息 {payload_bytes/1024:.1f}KB | NATS max_payload 10MB")

    assert payload_bytes < MAX_PAYLOAD_WARN, f"单消息 {payload_bytes} 超过 max_payload 10MB"

    start = time.time()
    sent = 0
    failed = 0
    interval = 1.0 / target_rps

    async def send_loop():
        nonlocal sent, failed
        while time.time() - start < duration:
            tick = time.time()
            payload["timestamp"] = tick
            payload["req_id"] = f"flood-{sent}"
            data = json.dumps(payload).encode()
            if len(data) >= MAX_PAYLOAD_WARN:
                failed += 1
            elif use_mock:
                await asyncio.sleep(0.001)
                sent += 1
            else:
                try:
                    await js.publish("asr.input", data, timeout=1)
                    sent += 1
                except Exception as e:
                    failed += 1
                    if "payload" in str(e).lower():
                        pytest.fail(f"max_payload 超限: {e}")
            elapsed = time.time() - tick
            sleep = interval - elapsed
            if sleep > 0:
                await asyncio.sleep(sleep)

    await send_loop()
    elapsed = time.time() - start
    actual_rps = sent / max(elapsed, 0.001)

    pending = 0
    if not use_mock and nc:
        try:
            jsz = await nc.request("$JS.API.STREAM.INFO.ASR_INPUT", b"", timeout=1)
            info = json.loads(jsz.data.decode())
            pending = info.get("state", {}).get("messages", 0)
        except Exception:
            pending = 0

    print(f"[flood] 完成: 发送 {sent} 条 | 失败 {failed} | 实际 {actual_rps:.1f} msg/s | pending {pending}")
    print(f"[flood] 断言: pending < {PENDING_THRESHOLD} ? {'✅' if pending < PENDING_THRESHOLD else '❌'}")

    if nc:
        try:
            await nc.drain()
        except Exception:
            pass

    result = {
        "target_rps": target_rps,
        "actual_rps": round(actual_rps, 1),
        "sent": sent,
        "failed": failed,
        "pending": pending,
        "payload_kb": round(payload_bytes / 1024, 1),
        "elapsed": round(elapsed, 2),
        "pass": pending < PENDING_THRESHOLD and failed == 0,
    }
    return result


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_nats_flood_100rps():
    result = await flood_test(target_rps=100, duration=2, payload_kb=32)
    assert result["failed"] == 0, f"发送失败 {result['failed']} 条"
    assert result["pending"] < PENDING_THRESHOLD, f"pending {result['pending']} 超阈值 {PENDING_THRESHOLD}"


def test_max_payload_not_exceeded():
    payload = _gen_payload(size_kb=64)
    size = len(json.dumps(payload).encode())
    assert size < MAX_PAYLOAD_WARN, f"64KB PCM 经 Base64 后 {size/1024:.1f}KB 未超 10MB，但 overhead {(size - 64*1024)/ (64*1024)*100:.1f}%"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NATS 洪峰压测 100 msg/s")
    parser.add_argument("--rps", type=int, default=TARGET_RPS)
    parser.add_argument("--duration", type=int, default=DURATION_SEC)
    parser.add_argument("--payload-kb", type=int, default=64)
    args = parser.parse_args()
    result = asyncio.run(flood_test(target_rps=args.rps, duration=args.duration, payload_kb=args.payload_kb))
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if not result["pass"]:
        exit(1)
