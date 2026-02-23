package com.darion.kpi.kpis;

public record LongestEventDTO(
        String id,
        String timestamp,
        String eventType,
        String sessionId,
        String actorId,
        String siteId,
        long durationMs,
        boolean success
) {}