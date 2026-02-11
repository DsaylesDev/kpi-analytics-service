package com.darion.kpi.kpis;

public interface KpiService {
    Object getKpi(KpiId id, KpiRequest req);
}
