package com.darion.kpi.kpis;

import java.time.Instant;

public record KpiRequest(
        Instant from,
        Instant to,
        String siteId,
        Integer topN,
        Integer limit
) {}
