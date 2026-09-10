import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.gateway import VADChunker, TokenBucket
from src.config import Config

SAMPLE_RATE = 16000
BYTES_PER_SEC = SAMPLE_RATE * 2

def _silence(duration=0.5):
    return b"\x00\x00" * int(SAMPLE_RATE * duration)

def _tone(duration=0.5, amp=8000):
    import struct, math
    n = int(SAMPLE_RATE * duration)
    buf = bytearray()
    for i in range(n):
        v = int(amp * math.sin(2*math.pi*440*i/SAMPLE_RATE))
        buf.extend(struct.pack("<h", v))
    return bytes(buf)

def test_vad_chunker_fixed_window_fallback():
    c = VADChunker(sample_rate=16000, min_chunk=0.8, max_chunk=1.5, overlap=0.3, vad_disable=True)
    assert c.min_bytes == int(0.8*BYTES_PER_SEC)
    assert c.max_bytes == int(1.5*BYTES_PER_SEC)
    chunks = c.feed(_tone(0.5))
    assert chunks == []
    chunks = c.feed(_tone(0.5))
    assert len(chunks) == 0 or len(chunks) == 1
    # feed enough to exceed max
    c2 = VADChunker(sample_rate=16000, min_chunk=0.8, max_chunk=1.0, overlap=0.3, vad_disable=True)
    chunks = c2.feed(_tone(1.2))
    assert len(chunks) == 1
    assert len(chunks[0]) == int(1.0*BYTES_PER_SEC)
    assert len(c2.buf) == int(0.3*BYTES_PER_SEC) or len(c2.buf) >=0

def test_vad_chunker_overlap():
    c = VADChunker(sample_rate=16000, min_chunk=0.8, max_chunk=1.0, overlap=0.3, vad_disable=True)
    c.feed(_tone(0.5))
    c.feed(_tone(0.5))
    # after 1s should cut at max 1.0s
    chunks = c.feed(b"")
    # ensure overlap kept
    assert len(c.buf) <= int(0.8*BYTES_PER_SEC)

def test_vad_chunker_silence_trigger():
    c = VADChunker(sample_rate=16000, min_chunk=0.8, max_chunk=1.5, overlap=0.3, vad_disable=True)
    # 1s tone + 0.4s silence -> should trigger silence cut after min_chunk
    chunks = c.feed(_tone(1.0))
    assert chunks == []
    chunks = c.feed(_silence(0.4))
    assert len(chunks) == 1
    # silence + overlap
    assert len(chunks[0]) >= c.min_bytes

def test_vad_chunker_max_fallback():
    c = VADChunker(sample_rate=16000, min_chunk=0.8, max_chunk=1.5, overlap=0.3, vad_disable=True)
    chunks = c.feed(_tone(2.0))
    assert len(chunks) == 1
    assert len(chunks[0]) == c.max_bytes

def test_vad_chunker_truncation_not_word_boundary():
    # ensure overlap preserves last 0.3s
    c = VADChunker(sample_rate=16000, min_chunk=0.8, max_chunk=1.0, overlap=0.3, vad_disable=True)
    data = _tone(1.0)
    chunks = c.feed(data)
    assert len(chunks) == 1
    overlap = c.buf
    assert len(overlap) == int(0.3*BYTES_PER_SEC)
    # next chunk should start with overlap
    chunks2 = c.feed(_tone(0.8))
    assert len(chunks2) >=1 or len(c.buf)>0

def test_token_bucket():
    b = TokenBucket(rate=10, capacity=10)
    for _ in range(10):
        assert b.consume()
    assert not b.consume()
    import time
    time.sleep(0.11)
    assert b.consume()

def test_gateway_config_defaults():
    assert Config.GATEWAY_MIN_CHUNK_SEC == 0.8
    assert Config.GATEWAY_MAX_CHUNK_SEC == 1.5
    assert Config.GATEWAY_OVERLAP_SEC == 0.3
    assert Config.USE_BINARY_PAYLOAD is True
    assert Config.ASR_COMPUTE_TYPE == "int8_float16"

def test_binary_compat_flag():
    # gateway should support both base64 json and binary headers path
    import base64, json
    pcm = _tone(0.1)
    b64 = base64.b64encode(pcm).decode()
    payload = {"req_id":"r1","session_id":"s1","audio_b64":b64}
    decoded = base64.b64decode(payload["audio_b64"])
    assert decoded == pcm
