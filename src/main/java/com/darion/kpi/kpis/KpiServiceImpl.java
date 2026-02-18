package com.darion.kpi.kpis;

import org.springframework.stereotype.Service;

@Service
public class KpiServiceImpl implements KpiService {

    private final EsKpiQueryClient es;

    public KpiServiceImpl(EsKpiQueryClient es) {
        this.es = es;
    }

    @Override
    public Object getKpi(KpiId id, KpiRequest req) {
        return switch (id) {

            case EVENT_TYPE_BREAKDOWN ->
                    es.eventTypeBreakdown(req.from(), req.to(), req.siteId());

            case EVENTS_PER_HOUR ->
                    es.eventsPerHour(req.from(), req.to(), req.siteId());

            case EVENTS_PER_HOUR_BY_TYPE -> {
                int topN = req.topN() == null ? 5 : req.topN();
                yield es.eventsPerHourByType(req.from(), req.to(), req.siteId(), topN);
            }

            case ERROR_RATE_PER_HOUR ->
                    es.errorRatePerHour(req.from(), req.to(), req.siteId());

            case DURATION_STATS_PER_HOUR ->
                    es.durationStatsPerHour(req.from(), req.to(), req.siteId());

            case SUCCESS_RATE ->
                    es.successRate(req.from(), req.to(), req.siteId());

            case TOP_ACTORS -> {
                int limit = req.limit() == null ? 10 : req.limit();
                yield es.topActors(req.from(), req.to(), req.siteId(), limit);
            }

            case SITE_VOLUME_AND_SUCCESS ->
                es.siteVolumeAndSuccess(req.from(), req.to(), req.siteId());


            case UNIQUE_ACTORS_PER_HOUR ->
                es.uniqueActorsPerHour(req.from(), req.to(), req.siteId());


            case UNIQUE_SESSIONS_PER_HOUR ->
                es.uniqueSessionsPerHour(req.from(), req.to(), req.siteId());

            case SUCCESS_RATE_BY_EVENT_TYPE ->
                    es.successRateByEventType(req.from(), req.to(), req.siteId());

        };
    }
}
