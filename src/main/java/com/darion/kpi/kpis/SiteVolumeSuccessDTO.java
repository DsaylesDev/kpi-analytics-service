package com.darion.kpi.kpis;

public record SiteVolumeSuccessDTO(String siteId, long total, long success, double successRate) {
}
