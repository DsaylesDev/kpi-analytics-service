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

    public List<DonutSliceDTO> eventTypeBreakdown(Instant from, Instant to, String siteId) {
        try {
            String body = buildAggQuery(from, to, siteId);

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

    private String buildAggQuery(Instant from, Instant to, String siteId) {
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

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
