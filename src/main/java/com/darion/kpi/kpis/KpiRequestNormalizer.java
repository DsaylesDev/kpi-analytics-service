package com.darion.kpi.kpis;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Component
public class KpiRequestNormalizer {

    // reasonable defaults
    private static final int DEFAULT_TOP_N = 5;
    private static final int DEFAULT_LIMIT = 10;

    // guardrails
    private static final int MAX_TOP_N = 25;
    private static final int MAX_LIMIT = 50;
    private static final Duration MAX_RANGE = Duration.ofDays(30); // keep queries sane

    public KpiRequest normalize(KpiRequest req) {
        if (req == null) throw new BadKpiRequestException("Request cannot be null");

        Instant from = req.from();
        Instant to = req.to();

        if (from == null || to == null) {
            throw new BadKpiRequestException("'from' and 'to' are required ISO-8601 instants");
        }
        if (from.isAfter(to)) {
            throw new BadKpiRequestException("'from' must be before 'to'");
        }
        if (Duration.between(from, to).compareTo(MAX_RANGE) > 0) {
            throw new BadKpiRequestException("Time range too large. Max range is " + MAX_RANGE.toDays() + " days");
        }

        String siteId = (req.siteId() == null || req.siteId().isBlank()) ? null : req.siteId().trim();

        Integer topN = req.topN();
        if (topN == null) topN = DEFAULT_TOP_N;
        if (topN < 1) topN = 1;
        if (topN > MAX_TOP_N) topN = MAX_TOP_N;

        Integer limit = req.limit();
        if (limit == null) limit = DEFAULT_LIMIT;
        if (limit < 1) limit = 1;
        if (limit > MAX_LIMIT) limit = MAX_LIMIT;

        return new KpiRequest(from, to, siteId, topN, limit);
    }
}
