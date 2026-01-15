package com.darion.kpi.events;

public record WarehouseEventRequest(
        String timestamp,
        String eventType,
        String sessionId,
        String actorId,
        String siteId,
        Long durationMs,
        Boolean success
) {}
