package com.darion.kpi.kpis;

public record EventTypeFailureDTO(String eventType, long total, long success, double successRate) {}