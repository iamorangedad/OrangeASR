try:
    from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST, REGISTRY
    HAS_PROM = True
except ImportError:
    HAS_PROM = False
    Counter = Gauge = Histogram = None
    generate_latest = lambda: b""
    CONTENT_TYPE_LATEST = "text/plain"
    REGISTRY = None

if HAS_PROM:
    gateway_ws_connections = Gauge("gateway_ws_connections", "Active WebSocket connections")
    gateway_backpressure_total = Counter("gateway_backpressure_total", "Backpressure events", ["type"])
    nats_publish_latency = Histogram("nats_publish_latency_seconds", "NATS publish latency", buckets=(0.005,0.01,0.02,0.05,0.1,0.2,0.5,1,2))
    worker_inference_seconds = Histogram("worker_inference_seconds", "ASR inference duration", ["compute_type"], buckets=(0.05,0.1,0.2,0.4,0.8,1,2,5))
    worker_queue_depth = Gauge("worker_queue_depth", "Worker queue depth", ["worker"])
    s3_upload_seconds = Histogram("s3_upload_seconds", "S3 upload duration", buckets=(0.02,0.05,0.1,0.2,0.5,1,2,5))
    llm_latency = Histogram("llm_latency_seconds", "LLM refine latency", buckets=(0.1,0.5,1,2,5,10,30))
    refiner_queue_depth = Gauge("refiner_queue_depth", "Refiner session buffer count")
    storage_buffer_depth = Gauge("storage_buffer_depth", "Storage bulk buffer depth")
else:
    class _Dummy:
        def inc(self, *a, **kw): pass
        def dec(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
    gateway_ws_connections = _Dummy()
    gateway_backpressure_total = _Dummy()
    nats_publish_latency = _Dummy()
    worker_inference_seconds = _Dummy()
    worker_queue_depth = _Dummy()
    s3_upload_seconds = _Dummy()
    llm_latency = _Dummy()
    refiner_queue_depth = _Dummy()
    storage_buffer_depth = _Dummy()

def metrics_content():
    if HAS_PROM:
        return generate_latest(), CONTENT_TYPE_LATEST
    return b"# no prometheus_client\n", "text/plain"

_health_state = {"nats": False, "model_loaded": False}

def set_health(nats_ok=None, model_loaded=None):
    if nats_ok is not None:
        _health_state["nats"] = bool(nats_ok)
    if model_loaded is not None:
        _health_state["model_loaded"] = bool(model_loaded)

def health_status():
    return dict(_health_state)

def start_metrics_server(port=8080):
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import json
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                data, ctype = metrics_content()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.end_headers()
                self.wfile.write(data)
            elif self.path in ("/healthz", "/health"):
                hs = health_status()
                ok = hs.get("nats", False) or hs.get("model_loaded", False)
                self.send_response(200 if ok else 503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(hs).encode())
            elif self.path in ("/ready", "/readiness"):
                hs = health_status()
                ok = hs.get("nats", False)
                self.send_response(200 if ok else 503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ready": ok, **hs}).encode())
            else:
                self.send_response(404)
                self.end_headers()
        def log_message(self, format, *args):
            return
    def _run():
        try:
            httpd = HTTPServer(("0.0.0.0", port), Handler)
            httpd.serve_forever()
        except Exception:
            pass
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t
