package com.darion.kpi.kpis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class KpiService {

    private final RestClient restClient;
    private final ObjectMapper mapper;

    public KpiService(RestClient restClient, ObjectMapper mapper) {
        this.restClient = restClient;
        this.mapper = mapper;
    }

    // KPI #1: Donut (event counts by type)
    public List<DonutSliceDTO> eventTypeBreakdown(Instant from, Instant to, String siteId) {
        try {
            String body = buildEventTypeBreakdownQuery(from, to, siteId);

            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(body);

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);

                JsonNode buckets = root.path("aggregations")
                        .path("by_event_type")
                        .path("buckets");

                List<DonutSliceDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String key = b.path("key").asText(null);
                        long count = b.path("doc_count").asLong(0);
                        if (key != null) {
                            out.add(new DonutSliceDTO(key, count));
                        }
                    }
                }
                return out;
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to compute event type breakdown", e);
        }
    }

    // KPI #2: Events per hour (date_histogram)
    public List<HourlyCountDTO> eventsPerHour(Instant from, Instant to, String siteId) {
        try {
            String body = buildEventsPerHourQuery(from, to, siteId);

            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(body);

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);

                JsonNode buckets = root.path("aggregations")
                        .path("events_per_hour")
                        .path("buckets");

                List<HourlyCountDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String hour = b.path("key_as_string").asText(null);
                        long count = b.path("doc_count").asLong(0);
                        if (hour != null) {
                            out.add(new HourlyCountDTO(hour, count));
                        }
                    }
                }
                return out;
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to compute events per hour", e);
        }
    }

    private String buildEventTypeBreakdownQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"size\":0,");
        sb.append("\"query\":{");
        sb.append("\"bool\":{");
        sb.append("\"filter\":[");
        sb.append("{\"range\":{\"timestamp\":{\"gte\":\"").append(from).append("\",\"lte\":\"").append(to).append("\"}}}");

        if (siteId != null && !siteId.isBlank()) {
            sb.append(",{\"term\":{\"siteId\":\"").append(escapeJson(siteId)).append("\"}}");
        }

        sb.append("]");
        sb.append("}");
        sb.append("},");
        sb.append("\"aggs\":{");
        sb.append("\"by_event_type\":{");
        sb.append("\"terms\":{\"field\":\"eventType\",\"size\":25}");
        sb.append("}");
        sb.append("}");
        sb.append("}");
        return sb.toString();
    }

    private String buildEventsPerHourQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"size\":0,");
        sb.append("\"query\":{");
        sb.append("\"bool\":{");
        sb.append("\"filter\":[");
        sb.append("{\"range\":{\"timestamp\":{\"gte\":\"").append(from).append("\",\"lte\":\"").append(to).append("\"}}}");

        if (siteId != null && !siteId.isBlank()) {
            sb.append(",{\"term\":{\"siteId\":\"").append(escapeJson(siteId)).append("\"}}");
        }

        sb.append("]");
        sb.append("}");
        sb.append("},");
        sb.append("\"aggs\":{");
        sb.append("\"events_per_hour\":{");
        sb.append("\"date_histogram\":{");
        sb.append("\"field\":\"timestamp\",");
        sb.append("\"fixed_interval\":\"1h\",");
        sb.append("\"min_doc_count\":0");
        sb.append("}");
        sb.append("}");
        sb.append("}");
        sb.append("}");
        return sb.toString();
    }

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
    public SuccessRateDTO successRate(Instant from, Instant to, String siteId) {
        try {
            String body = buildSuccessRateQuery(from, to, siteId);

            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(body);

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);

                long total = root.path("hits").path("total").path("value").asLong(0);
                long success = root.path("aggregations")
                        .path("successful_events")
                        .path("doc_count")
                        .asLong(0);

                double rate = total == 0 ? 0.0 : (success * 100.0) / total;
                return new SuccessRateDTO(total, success, Math.round(rate * 100.0) / 100.0);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to compute success rate KPI", e);
        }
    }
    private String buildSuccessRateQuery(Instant from, Instant to, String siteId) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"query\":{");
        sb.append("\"bool\":{");
        sb.append("\"filter\":[");
        sb.append("{\"range\":{\"timestamp\":{\"gte\":\"").append(from).append("\",\"lte\":\"").append(to).append("\"}}}");

        if (siteId != null && !siteId.isBlank()) {
            sb.append(",{\"term\":{\"siteId\":\"").append(escapeJson(siteId)).append("\"}}");
        }

        sb.append("]");
        sb.append("}");
        sb.append("},");
        sb.append("\"aggs\":{");
        sb.append("\"successful_events\":{");
        sb.append("\"filter\":{\"term\":{\"success\":true}}");
        sb.append("}");
        sb.append("}");
        sb.append("}");
        return sb.toString();
    }
    public List<LeaderboardEntryDTO> topActors(
            Instant from,
            Instant to,
            String siteId,
            int limit
    ) {
        try {
            String body = buildTopActorsQuery(from, to, siteId, limit);

            Request req = new Request("POST", "/warehouse_events/_search");
            req.setJsonEntity(body);

            Response resp = restClient.performRequest(req);

            try (InputStream is = resp.getEntity().getContent()) {
                JsonNode root = mapper.readTree(is);

                JsonNode buckets = root.path("aggregations")
                        .path("top_actors")
                        .path("buckets");

                List<LeaderboardEntryDTO> out = new ArrayList<>();
                if (buckets.isArray()) {
                    for (JsonNode b : buckets) {
                        String actorId = b.path("key").asText(null);
                        long count = b.path("doc_count").asLong(0);
                        if (actorId != null) {
                            out.add(new LeaderboardEntryDTO(actorId, count));
                        }
                    }
                }
                return out;
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to compute top actors leaderboard", e);
        }
    }
    private String buildTopActorsQuery(
            Instant from,
            Instant to,
            String siteId,
            int limit
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"size\":0,");
        sb.append("\"query\":{");
        sb.append("\"bool\":{");
        sb.append("\"filter\":[");
        sb.append("{\"range\":{\"timestamp\":{\"gte\":\"").append(from).append("\",\"lte\":\"").append(to).append("\"}}}");

        if (siteId != null && !siteId.isBlank()) {
            sb.append(",{\"term\":{\"siteId\":\"").append(escapeJson(siteId)).append("\"}}");
        }

        sb.append("]");
        sb.append("}");
        sb.append("},");
        sb.append("\"aggs\":{");
        sb.append("\"top_actors\":{");
        sb.append("\"terms\":{");
        sb.append("\"field\":\"actorId\",");
        sb.append("\"size\":").append(limit).append(",");
        sb.append("\"order\":{\"_count\":\"desc\"}");
        sb.append("}");
        sb.append("}");
        sb.append("}");
        sb.append("}");
        return sb.toString();
    }



}
