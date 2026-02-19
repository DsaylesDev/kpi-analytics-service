package com.darion.kpi.kpis;

public record EventTypeDurationStatsDTO(String eventType, double avgDurationMs, double p95DurationMs) {}
