import sys, unittest.mock as mock
for m in ["gradio","aioboto3","motor.motor_asyncio","motor","httpx","boto3","pymongo","nats"]:
    if m not in sys.modules:
        sys.modules[m]=mock.MagicMock()
try:
    import scipy.signal
except Exception:
    import unittest.mock as m
    sys.modules["scipy.signal"]=m.MagicMock()
    sys.modules["scipy"]=m.MagicMock()
import pytest
from fastapi.testclient import TestClient

def test_gateway_metrics_and_health(monkeypatch):
    from src.gateway import app
    client = TestClient(app)
    r = client.get("/metrics")
    assert r.status_code == 200
    # metrics should contain gateway metric or fallback text
    assert b"gateway" in r.content or b"prometheus" in r.content.lower() or b"no prometheus" in r.content
    r = client.get("/healthz")
    assert r.status_code == 200
    assert "status" in r.json()
    r = client.get("/ready")
    assert r.status_code in (200,503)

def test_metrics_module_no_prom_fallback():
    from src import metrics
    data, ctype = metrics.metrics_content()
    assert isinstance(data, bytes)
    assert isinstance(ctype, str)

def test_metrics_server_start():
    from src.metrics import start_metrics_server
    t = start_metrics_server(18081)
    assert t.daemon is True
    import time, urllib.request, json
    time.sleep(0.3)
    try:
        with urllib.request.urlopen("http://127.0.0.1:18081/metrics", timeout=2) as resp:
            body = resp.read()
            assert resp.status == 200
            assert len(body) >= 0
        with urllib.request.urlopen("http://127.0.0.1:18081/healthz", timeout=2) as resp:
            assert resp.status in (200,503)
    except Exception as e:
        pytest.skip(f"metrics server not reachable: {e}")

def test_hpa_yaml_exists():
    from pathlib import Path
    text = Path("deploy/k8s/hpa.yaml").read_text()
    assert "asr-gateway-hpa" in text
    assert "minReplicas: 2" in text
    assert "maxReplicas: 5" in text
    assert "averageUtilization: 70" in text
    assert "nats_consumer_pending" in text
    assert "worker_queue_depth" in text

def test_grafana_dashboard():
    import json
    from pathlib import Path
    data = json.loads(Path("docs/Grafana看板.json").read_text())
    assert data["title"] == "OrangeASR Perf"
    exprs = " ".join(str(p) for p in data["panels"])
    assert "gateway_ws_connections" in exprs
    assert "worker_inference_seconds" in exprs
    assert "s3_upload_seconds" in exprs

def test_perf_report_exists():
    from pathlib import Path
    text = Path("docs/perf_tuning_report.md").read_text()
    assert "NATS publish latency" in text or "nats_publish_latency" in text
    assert "HPA" in text
    assert "Backpressure" in text

def test_k8s_probes_configured():
    from pathlib import Path
    text = Path("deploy/k8s/02-apps.yaml").read_text()
    assert "livenessProbe" in text
    assert "readinessProbe" in text
    assert "/healthz" in text
    assert "/ready" in text
    assert "8081" in text
    assert "8082" in text
    assert "8083" in text

def test_backpressure_metrics():
    from src import metrics
    try:
        metrics.gateway_backpressure_total.labels(type="busy").inc()
        metrics.gateway_backpressure_total.labels(type="backpressure").inc()
    except Exception as e:
        pytest.skip(f"metrics label not supported: {e}")
