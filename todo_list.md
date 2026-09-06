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

The `asr-worker` claims an entire dedicated GPU (`[nvidia.com/gpu](https://nvidia.com/gpu): 1`), which leads to low resource utilization when running lightweight Whisper models.

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