package com.darion.kpi.kpis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Component
public class EsKpiQueryClient {

    private final RestClient restClient;
    private final ObjectMapper mapper;

    public EsKpiQueryClient(RestClient restClient, ObjectMapper mapper) {
        this.restClient = restClient;
        this.mapper = mapper;
    }

    // ---------------- KPI: Event Type Breakdown (donut) ----------------

    public List<DonutSliceDTO> eventTypeBreakdown(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildEventTypeBreakdownQuery(from, to, siteId));

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode buckets = root.path("aggregations").path("by_event_type").path("buckets");

                List<DonutSliceDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String key = b.path("key").asText(null);
                        long count = b.path("doc_count").asLong(0);
                        if (key != null) out.add(new DonutSliceDTO(key, count));
                    }
                }
                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed EVENT_TYPE_BREAKDOWN KPI", e);
        }
    }

    // ---------------- KPI: Events Per Hour ----------------

    public List<HourlyCountDTO> eventsPerHour(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildEventsPerHourQuery(from, to, siteId));

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode buckets = root.path("aggregations").path("events_per_hour").path("buckets");

                List<HourlyCountDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String hour = b.path("key_as_string").asText(null);
                        long count = b.path("doc_count").asLong(0);
                        if (hour != null) out.add(new HourlyCountDTO(hour, count));
                    }
                }
                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed EVENTS_PER_HOUR KPI", e);
        }
    }

    // ---------------- KPI: Stacked Events Per Hour by Type (topN + OTHER) ----------------

    public List<HourlyStackedDTO> eventsPerHourByType(Instant from, Instant to, String siteId, int topN) {
        try {
            if (topN < 1) topN = 1;

            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildEventsPerHourByTypeQuery(from, to, siteId));

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode hourBuckets = root.path("aggregations").path("events_per_hour").path("buckets");

                List<HourlyStackedDTO> out = new ArrayList<>();

                if (hourBuckets.isArray()) {
                    for (JsonNode hb : hourBuckets) {
                        String hour = hb.path("key_as_string").asText(null);
                        long total = hb.path("doc_count").asLong(0);

                        List<DonutSliceDTO> byType = new ArrayList<>();
                        JsonNode typeBuckets = hb.path("by_type").path("buckets");

                        if (typeBuckets.isArray()) {
                            for (JsonNode tb : typeBuckets) {
                                String label = tb.path("key").asText(null);
                                long count = tb.path("doc_count").asLong(0);
                                if (label != null) byType.add(new DonutSliceDTO(label, count));
                            }
                        }

                        // sort desc
                        byType.sort((a, b) -> Long.compare(b.value(), a.value()));

                        List<DonutSliceDTO> trimmed = new ArrayList<>();
                        long other = 0;

                        for (int i = 0; i < byType.size(); i++) {
                            DonutSliceDTO slice = byType.get(i);
                            if (i < topN) trimmed.add(slice);
                            else other += slice.value();
                        }

                        if (other > 0) trimmed.add(new DonutSliceDTO("OTHER", other));

                        if (hour != null) out.add(new HourlyStackedDTO(hour, total, trimmed));
                    }
                }

                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed EVENTS_PER_HOUR_BY_TYPE KPI", e);
        }
    }

    // ---------------- KPI: Error Rate Per Hour ----------------

    public List<HourlyErrorRateDTO> errorRatePerHour(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildErrorRatePerHourQuery(from, to, siteId));

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode hourBuckets = root.path("aggregations").path("errors_per_hour").path("buckets");

                List<HourlyErrorRateDTO> out = new ArrayList<>();

                if (hourBuckets.isArray()) {
                    for (JsonNode hb : hourBuckets) {
                        String hour = hb.path("key_as_string").asText(null);
                        long total = hb.path("doc_count").asLong(0);
                        long errors = hb.path("errors_only").path("doc_count").asLong(0);

                        double rate = total == 0 ? 0.0 : (errors * 100.0) / total;
                        rate = Math.round(rate * 100.0) / 100.0;

                        if (hour != null) out.add(new HourlyErrorRateDTO(hour, total, errors, rate));
                    }
                }

                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed ERROR_RATE_PER_HOUR KPI", e);
        }
    }

    // ---------------- KPI: Duration Stats Per Hour (avg + p95) ----------------

    public List<HourlyDurationStatsDTO> durationStatsPerHour(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildDurationStatsPerHourQuery(from, to, siteId));

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode hourBuckets = root.path("aggregations").path("duration_per_hour").path("buckets");

                List<HourlyDurationStatsDTO> out = new ArrayList<>();

                if (hourBuckets.isArray()) {
                    for (JsonNode hb : hourBuckets) {
                        String hour = hb.path("key_as_string").asText(null);

                        double avg = hb.path("avg_duration").path("value").asDouble(0.0);

                        JsonNode p95Node = hb.path("p95_duration").path("values").path("95.0");
                        double p95 = (p95Node.isMissingNode() || p95Node.isNull()) ? 0.0 : p95Node.asDouble(0.0);

                        avg = Math.round(avg * 100.0) / 100.0;
                        p95 = Math.round(p95 * 100.0) / 100.0;

                        if (hour != null) out.add(new HourlyDurationStatsDTO(hour, avg, p95));
                    }
                }

                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed DURATION_STATS_PER_HOUR KPI", e);
        }
    }

    // ---------------- KPI: Success Rate (overall) ----------------

    public SuccessRateDTO successRate(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildSuccessRateQuery(from, to, siteId));

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);

                long total = root.path("hits").path("total").path("value").asLong(0);
                long success = root.path("aggregations").path("successful_events").path("doc_count").asLong(0);

                double rate = total == 0 ? 0.0 : (success * 100.0) / total;
                rate = Math.round(rate * 100.0) / 100.0;

                return new SuccessRateDTO(total, success, rate);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed SUCCESS_RATE KPI", e);
        }
    }

    // ---------------- KPI: Top Actors ----------------

    public List<LeaderboardEntryDTO> topActors(Instant from, Instant to, String siteId, int limit) {
        try {
            if (limit < 1) limit = 1;

            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildTopActorsQuery(from, to, siteId, limit));

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode buckets = root.path("aggregations").path("top_actors").path("buckets");

                List<LeaderboardEntryDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String actorId = b.path("key").asText(null);
                        long count = b.path("doc_count").asLong(0);
                        if (actorId != null) out.add(new LeaderboardEntryDTO(actorId, count));
                    }
                }
                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed TOP_ACTORS KPI", e);
        }
    }

    public List<SiteVolumeSuccessDTO> siteVolumeAndSuccess(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildSiteVolumeAndSuccessQuery(from, to, siteId));

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode buckets = root.path("aggregations").path("by_site").path("buckets");

                List<SiteVolumeSuccessDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String s = b.path("key").asText(null);
                        long total = b.path("doc_count").asLong(0);
                        long success = b.path("success_only").path("doc_count").asLong(0);

                        double rate = total == 0 ? 0.0 : (success * 100.0) / total;
                        rate = Math.round(rate * 100.0) / 100.0;

                        if (s != null) out.add(new SiteVolumeSuccessDTO(s, total, success, rate));
                    }
                }
                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed SITE_VOLUME_AND_SUCCESS KPI", e);
        }

    }
    public List<HourlyUniqueCountDTO> uniqueActorsPerHour(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildUniqueActorsPerHourQuery(from, to, siteId));
            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode buckets = root.path("aggregations").path("per_hour").path("buckets");

                List<HourlyUniqueCountDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String hour = b.path("key_as_string").asText(null);
                        long unique = b.path("unique_actors").path("value").asLong(0);
                        if (hour != null) out.add(new HourlyUniqueCountDTO(hour, unique));
                    }
                }
                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed UNIQUE_ACTORS_PER_HOUR KPI", e);
        }
    }
    public List<HourlyUniqueSessionsDTO> uniqueSessionsPerHour(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildUniqueSessionsPerHourQuery(from, to, siteId));
            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode buckets = root.path("aggregations").path("per_hour").path("buckets");

                List<HourlyUniqueSessionsDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String hour = b.path("key_as_string").asText(null);
                        long unique = b.path("unique_sessions").path("value").asLong(0);
                        if (hour != null) out.add(new HourlyUniqueSessionsDTO(hour, unique));
                    }
                }
                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed UNIQUE_SESSIONS_PER_HOUR KPI", e);
        }
    }
    public List<EventTypeSuccessDTO> successRateByEventType(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildSuccessRateByEventTypeQuery(from, to, siteId));
            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode buckets = root.path("aggregations").path("by_type").path("buckets");

                List<EventTypeSuccessDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String type = b.path("key").asText(null);
                        long total = b.path("doc_count").asLong(0);
                        long success = b.path("success_only").path("doc_count").asLong(0);
                        double rate = total == 0 ? 0.0 : (success * 100.0) / total;
                        rate = Math.round(rate * 100.0) / 100.0;
                        if (type != null) out.add(new EventTypeSuccessDTO(type, total, success, rate));
                    }
                }
                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed SUCCESS_RATE_BY_EVENT_TYPE KPI", e);
        }
    }
    public List<EventTypeDurationStatsDTO> durationStatsByEventType(Instant from, Instant to, String siteId) {
        try {
            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(buildDurationStatsByEventTypeQuery(from, to, siteId));
            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);
                JsonNode buckets = root.path("aggregations").path("by_type").path("buckets");

                List<EventTypeDurationStatsDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String type = b.path("key").asText(null);
                        double avg = b.path("avg_duration").path("value").asDouble(0.0);
                        JsonNode p95Node = b.path("p95_duration").path("values").path("95.0");
                        double p95 = (p95Node.isMissingNode() || p95Node.isNull()) ? 0.0 : p95Node.asDouble(0.0);
                        avg = Math.round(avg * 100.0) / 100.0;
                        p95 = Math.round(p95 * 100.0) / 100.0;
                        if (type != null) out.add(new EventTypeDurationStatsDTO(type, avg, p95));
                    }
                }
                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed DURATION_STATS_BY_EVENT_TYPE KPI", e);
        }
    }

    private String buildDurationStatsByEventTypeQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"by_type\":{");
        sb.append("\"terms\":{\"field\":\"eventType\",\"size\":25,\"order\":{\"_count\":\"desc\"}},");
        sb.append("\"aggs\":{");
        sb.append("\"avg_duration\":{\"avg\":{\"field\":\"durationMs\"}},");
        sb.append("\"p95_duration\":{\"percentiles\":{\"field\":\"durationMs\",\"percents\":[95]}}");
        sb.append("}}}}");
        return sb.toString();
    }


    private String buildSuccessRateByEventTypeQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"by_type\":{");
        sb.append("\"terms\":{\"field\":\"eventType\",\"size\":25,\"order\":{\"_count\":\"desc\"}},");
        sb.append("\"aggs\":{\"success_only\":{\"filter\":{\"term\":{\"success\":true}}}}");
        sb.append("}}}");
        return sb.toString();
    }

    private String buildUniqueSessionsPerHourQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"per_hour\":{");
        sb.append("\"date_histogram\":{\"field\":\"timestamp\",\"fixed_interval\":\"1h\",\"min_doc_count\":0},");
        sb.append("\"aggs\":{\"unique_sessions\":{\"cardinality\":{\"field\":\"sessionId\"}}}");
        sb.append("}}}");
        return sb.toString();
    }


    private String buildUniqueActorsPerHourQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"per_hour\":{");
        sb.append("\"date_histogram\":{\"field\":\"timestamp\",\"fixed_interval\":\"1h\",\"min_doc_count\":0},");
        sb.append("\"aggs\":{\"unique_actors\":{\"cardinality\":{\"field\":\"actorId\"}}}");
        sb.append("}}}");
        return sb.toString();
    }

    private String buildSiteVolumeAndSuccessQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"by_site\":{");
        sb.append("\"terms\":{\"field\":\"siteId\",\"size\":50,\"order\":{\"_count\":\"desc\"}},");
        sb.append("\"aggs\":{\"success_only\":{\"filter\":{\"term\":{\"success\":true}}}}");
        sb.append("}}}");
        return sb.toString();
    }

    private String buildEventTypeBreakdownQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"by_event_type\":{\"terms\":{\"field\":\"eventType\",\"size\":25}}}}");
        return sb.toString();
    }

    private String buildEventsPerHourQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"events_per_hour\":{\"date_histogram\":{\"field\":\"timestamp\",\"fixed_interval\":\"1h\",\"min_doc_count\":0}}}}");
        return sb.toString();
    }

    private String buildEventsPerHourByTypeQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"events_per_hour\":{");
        sb.append("\"date_histogram\":{\"field\":\"timestamp\",\"fixed_interval\":\"1h\",\"min_doc_count\":0},");
        sb.append("\"aggs\":{\"by_type\":{\"terms\":{\"field\":\"eventType\",\"size\":25,\"order\":{\"_count\":\"desc\"}}}}");
        sb.append("}}}");
        return sb.toString();
    }

    private String buildErrorRatePerHourQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"errors_per_hour\":{");
        sb.append("\"date_histogram\":{\"field\":\"timestamp\",\"fixed_interval\":\"1h\",\"min_doc_count\":0},");
        sb.append("\"aggs\":{\"errors_only\":{\"filter\":{\"term\":{\"success\":false}}}}");
        sb.append("}}}");
        return sb.toString();
    }

    private String buildDurationStatsPerHourQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"duration_per_hour\":{");
        sb.append("\"date_histogram\":{\"field\":\"timestamp\",\"fixed_interval\":\"1h\",\"min_doc_count\":0},");
        sb.append("\"aggs\":{");
        sb.append("\"avg_duration\":{\"avg\":{\"field\":\"durationMs\"}},");
        sb.append("\"p95_duration\":{\"percentiles\":{\"field\":\"durationMs\",\"percents\":[95]}}");
        sb.append("}");
        sb.append("}}}");
        return sb.toString();
    }

    private String buildSuccessRateQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"successful_events\":{\"filter\":{\"term\":{\"success\":true}}}}}");
        return sb.toString();
    }

    private String buildTopActorsQuery(Instant from, Instant to, String siteId, int limit) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"size\":0,");
        sb.append("\"query\":{\"bool\":{\"filter\":[");
        sb.append(rangeTimestamp(from, to));
        if (hasText(siteId)) sb.append(",").append(term("siteId", siteId));
        sb.append("]}},");
        sb.append("\"aggs\":{\"top_actors\":{\"terms\":{\"field\":\"actorId\",\"size\":").append(limit).append(",\"order\":{\"_count\":\"desc\"}}}}}");
        return sb.toString();
    }

    private boolean hasText(String s) {
        return s != null && !s.isBlank();
    }

    private String rangeTimestamp(Instant from, Instant to) {
        return "{\"range\":{\"timestamp\":{\"gte\":\"" + from + "\",\"lte\":\"" + to + "\"}}}";
    }

    private String term(String field, String value) {
        return "{\"term\":{\"" + field + "\":\"" + escapeJson(value) + "\"}}";
    }

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
