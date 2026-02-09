# KPI Analytics Service

A Spring Boot–based analytics service backed by Elasticsearch that ingests warehouse-style event data and exposes multiple KPI endpoints for operational dashboards.

This project was built to demonstrate backend ownership of:
- Event ingestion
- Time-based aggregations
- Operational KPIs (volume, quality, reliability)
- Elasticsearch-backed analytics APIs

---

## What This Service Does

The service accepts raw event data (e.g. PICK, PACK, SORT, ERROR) and computes analytics over a time range, optionally scoped by site.

It exposes REST endpoints designed to be consumed by dashboards (charts, KPIs, leaderboards).

---

## Event Model (Simplified)

Each event represents an operational action and includes:

- `timestamp` – when the event occurred
- `eventType` – type of action (PICK, PACK, SORT, ERROR, etc.)
- `sessionId` – logical session or workflow
- `actorId` – user or robot that generated the event
- `siteId` – warehouse/site identifier
- `durationMs` – execution time
- `success` – whether the event completed successfully

Events are stored in Elasticsearch and queried using aggregations.

---

## Implemented KPIs

### 1. Event Type Breakdown (Donut)
**Endpoint**

**Description**
Aggregates events by `eventType` to show distribution of activity.

**Use case**
Quick overview of operational mix (PICK vs PACK vs ERROR).

---

### 2. Events Per Hour
**Endpoint**

**Description**
Buckets events into hourly intervals using a date histogram.

**Use case**
Visualize workload volume over time.

---

### 3. Stacked Events Per Hour by Type
**Endpoint**

**Description**
Hourly buckets with sub-buckets per `eventType`. Supports `topN` filtering with an `OTHER` bucket.

**Use case**
Stacked bar charts for operational dashboards.

---

### 4. Error Rate Per Hour
**Endpoint**

**Description**
Calculates error count and error percentage per hour using filtered aggregations.

**Use case**
Reliability monitoring and incident analysis.

---

### 5. Duration Metrics Per Hour (Avg + P95)
**Endpoint**

**Description**
Computes average and 95th percentile execution time per hour.

**Use case**
Performance and latency analysis.

---

### 6. Success Rate KPI
**Endpoint**

**Description**
Returns total events, successful events, and overall success percentage.

**Use case**
High-level system health indicator.

---

### 7. Top Actors Leaderboard
**Endpoint**

**Description**
Ranks users/robots by event volume within a time window.

**Use case**
Identify high-activity actors or automation hotspots.

---

## Technology Stack

- **Java 21**
- **Spring Boot 3.4**
- **Elasticsearch 8.x**
- **Jackson** for JSON parsing
- **Docker** for local Elasticsearch

The KPI queries are executed using Elasticsearch `_search` requests with aggregations and parsed directly for stability and version compatibility.

---

## Running Locally

### 1. Start Elasticsearch
```bash
docker compose up -d
http://localhost:9200
http://localhost:8080
