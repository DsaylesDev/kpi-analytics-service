package com.darion.kpi.kpis;

public record EventTypeSuccessDTO(String eventType, long total, long success, double successRate) {}
