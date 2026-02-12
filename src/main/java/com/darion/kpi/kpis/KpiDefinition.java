package com.darion.kpi.kpis;

public record KpiDefinition(
        KpiId id,
        String displayName,
        String description,
        ChartType chartType
) {}
