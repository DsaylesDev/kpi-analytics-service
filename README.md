# KPI Analytics Service

A Spring Boot analytics backend that computes real-time KPIs from warehouse event data stored in Elasticsearch.

This project is intentionally designed using a production-style architecture similar to enterprise Warehouse Execution Systems (WES):

- Enum-driven KPI routing
- Switch-based service dispatch
- Dedicated Elasticsearch query client
- Request normalization + validation
- Clean separation of concerns
- Extensible KPI registry

---

## 🚀 Architecture Overview
HTTP Request
↓
KpiController
↓
KpiRequestNormalizer
↓
KpiService (interface)
↓
KpiServiceImpl (switch-based routing)
↓
EsKpiQueryClient (Elasticsearch queries)
↓
Elasticsearch

### Design Goals

- Single unified KPI endpoint (`/kpis/{KPI_ID}`)
- Enum-based routing (ALL_CAPS IDs)
- Switch-driven service pattern (enterprise style)
- Dedicated Elasticsearch aggregation layer
- Immutable DTO responses (Java records)
- Safe request validation and normalization

---

## 🛠 Tech Stack

- Java 21
- Spring Boot 3.4.x
- Elasticsearch 8.x
- Jackson
- Maven
- Docker (for local ES)

---

## 🏃 Running Locally

### 1️⃣ Start Elasticsearch (Docker)

```bash
docker run -d \
  --name es \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  docker.elastic.co/elasticsearch/elasticsearch:8.13.4

Verify it is running:
curl http://localhost:9200

2️⃣ Run the Spring Boot App
mvn spring-boot:run

Application runs on:
http://localhost:8080

📥 Ingesting Sample Events

Example PowerShell request:
$body = @{
  timestamp  = "2026-01-15T16:00:00Z"
  eventType  = "PICK"
  sessionId  = "sess-1001"
  actorId    = "user-12"
  siteId     = "PHL1"
  durationMs = 1830
  success    = $true
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/events" `
  -ContentType "application/json" `
  -Body $body

📊 Calling KPIs

Unified endpoint:
GET /kpis/{KPI_ID}

Example:
curl "http://localhost:8080/kpis/EVENTS_PER_HOUR?from=2026-01-15T15:00:00Z&to=2026-01-15T19:00:00Z&siteId=PHL1"

🔎 Required Parameters
| Parameter | Type             | Required |
| --------- | ---------------- | -------- |
| from      | ISO-8601 Instant | Yes      |
| to        | ISO-8601 Instant | Yes      |
| siteId    | String           | No       |
| topN      | Integer          | No       |
| limit     | Integer          | No       |

📈 Available KPIs
Core Analytics

EVENT_TYPE_BREAKDOWN

EVENTS_PER_HOUR

EVENTS_PER_HOUR_BY_TYPE

ERROR_RATE_PER_HOUR

DURATION_STATS_PER_HOUR

SUCCESS_RATE

TOP_ACTORS

Advanced KPIs

SITE_VOLUME_AND_SUCCESS

UNIQUE_ACTORS_PER_HOUR

UNIQUE_SESSIONS_PER_HOUR

SUCCESS_RATE_BY_EVENT_TYPE

DURATION_STATS_BY_EVENT_TYPE

TOP_SESSIONS_BY_EVENT_COUNT

TOP_EVENT_TYPES

THROUGHPUT_PER_MINUTE

ERROR_TYPES_BREAKDOWN

TOP_LONGEST_EVENTS

ACTOR_ACTIVITY_SUMMARY

📚 KPI Definitions Endpoint

Retrieve metadata for all KPIs:
GET /kpis/definitions

Example response:
[
  {
    "id": "EVENT_TYPE_BREAKDOWN",
    "displayName": "Event Type Breakdown",
    "description": "Distribution of events grouped by type.",
    "chartType": "DONUT"
  }
]

🛡 Request Validation

All KPI requests pass through KpiRequestNormalizer, which:

Validates from < to

Prevents excessive time ranges

Applies safe defaults

Clamps topN and limit

Returns clean HTTP 400 errors for invalid requests

Example invalid request:
/kpis/EVENTS_PER_HOUR?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z

Returns:
{
  "status": 400,
  "error": "Bad Request",
  "message": "'from' must be before 'to'"
}

📁 Project Structure
kpis/
 ├── KpiController
 ├── KpiService
 ├── KpiServiceImpl
 ├── KpiId
 ├── KpiRegistry
 ├── KpiRequest
 ├── KpiRequestNormalizer
 ├── EsKpiQueryClient
 ├── DTO records

🧠 Why This Architecture?

This service mirrors enterprise analytics systems by:

Centralizing KPI routing

Separating HTTP, business logic, and data layers

Using Elasticsearch aggregations efficiently

Supporting extensible KPI definitions

Enabling daily KPI expansion without architectural change

🔮 Future Improvements

Response caching

Async KPI execution

Strategy pattern (replace switch)

OpenAPI documentation

Integration test suite

Performance benchmarking

Multi-index support

Role-based KPI access
