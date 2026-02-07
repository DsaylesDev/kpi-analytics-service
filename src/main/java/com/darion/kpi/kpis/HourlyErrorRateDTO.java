package com.darion.kpi.kpis;

public record HourlyErrorRateDTO(String hour, long total, long errors, double errorRate) {}
