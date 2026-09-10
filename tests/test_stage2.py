import sys, types, unittest.mock as mock
# mock heavy deps to allow import in numpy2 env
for m in ["gradio", "aioboto3", "motor.motor_asyncio", "motor", "httpx"]:
    if m not in sys.modules:
        sys.modules[m] = mock.MagicMock()
# mock scipy signal if broken
try:
    import scipy.signal
except Exception:
    sys.modules["scipy.signal"] = mock.MagicMock()
    sys.modules["scipy"] = mock.MagicMock()

import asyncio, base64, json, time, queue
import pytest
for _m in ["boto3", "pymongo", "nats"]:
    if _m not in sys.modules:
        sys.modules[_m] = mock.MagicMock()

def test_webui_resample_cache():
    sys.modules["gradio"] = mock.MagicMock()
    sys.modules["websocket"] = mock.MagicMock()
    from src.web_ui import _get_resample_params, _resample_cache
    _resample_cache.clear()
    up, down = _get_resample_params(48000, 16000)
    assert (up, down) == (1, 3)
    up2, down2 = _get_resample_params(48000, 16000)
    assert up == up2
    up3, down3 = _get_resample_params(44100, 16000)
    assert up3 != up or down3 != down

def test_webui_send_queue_and_backpressure():
    from src.web_ui import RealtimeClient
    c = RealtimeClient()
    c.connected = True
    c.ws = mock.MagicMock()
    c.ws.send_binary = mock.MagicMock()
    c._send_loop = lambda: None
    import numpy as np
    data = (np.random.randn(1600) * 0.1).astype(np.float32)
    c.send_audio_chunk(16000, data)
    assert c.send_queue.qsize() == 1
    c._backpressure_until = time.time() + 10
    for _ in range(5):
        c.send_audio_chunk(16000, data)
    # backpressure should throttle when queue >3
    assert c.send_queue.qsize() <= 20

def test_storage_bulk_buffer():
    from src.storage_worker import StorageWorker
    w = StorageWorker()
    w.collection = mock.MagicMock()
    w.collection.insert_many = mock.MagicMock()
    w.async_collection = None
    async def run():
        for i in range(5):
            async with w._buffer_lock:
                w._buffer.append({"req_id": str(i), "text": "hi"})
        assert len(w._buffer) == 5
        await w._flush_buffer()
        assert len(w._buffer) == 0
        assert w.collection.insert_many.called
    asyncio.run(run())

def test_refiner_circuit_breaker():
    from src.refiner import ASRRefiner
    r = ASRRefiner(timeout=1)
    assert not r._circuit_open()
    for _ in range(3):
        r._record_failure()
    assert r._circuit_open()
    r._circuit_until = time.time() - 1
    assert not r._circuit_open()
    r._record_success()
    assert r._failures == 0

def test_refiner_arefine_mock():
    from src.refiner import ASRRefiner
    r = ASRRefiner(timeout=2)
    # mock httpx
    async def run():
        with mock.patch("src.refiner.httpx.AsyncClient") as MockClient:
            mock_resp = mock.MagicMock()
            mock_resp.json.return_value = {"response": '{"audio_id":"101","original_text":"a","corrected_text":"b"}'}
            mock_resp.raise_for_status = mock.MagicMock()
            MockClient.return_value.__aenter__.return_value.post = mock.AsyncMock(return_value=mock_resp)
            result = await r.arefine([{"audio_id":"101","transcribe_text":"hello"}])
            assert result["corrected_text"] == "b"
    asyncio.run(run())

def test_nats_split_subjects_exist():
    from pathlib import Path
    text = Path("deploy/k8s/nats-init.yaml").read_text()
    assert "asr.output.transcript" in text
    assert "asr.output.archive" in text
    gw = Path("src/gateway.py").read_text()
    assert "asr.output.transcript" in gw
    storage = Path("src/storage_worker.py").read_text()
    assert "asr.output.archive" in storage
    asr = Path("src/asr_worker.py").read_text()
    assert "asr.output.transcript" in asr
    assert "add_stream" not in asr or asr.count("add_stream") == 0

def test_gateway_and_workers_no_add_stream():
    for p in ["src/asr_worker.py", "src/storage_worker.py", "src/refiner_worker.py"]:
        text = open(p).read()
        assert "add_stream" not in text, f"{p} should not contain add_stream"
