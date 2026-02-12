package com.darion.kpi.kpis;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KpiRegistry {

    private final List<KpiDefinition> definitions = List.of(
            new KpiDefinition(
                    KpiId.EVENT_TYPE_BREAKDOWN,
                    "Event Type Breakdown",
                    "Distribution of events grouped by type.",
                    ChartType.DONUT
            ),
            new KpiDefinition(
                    KpiId.EVENTS_PER_HOUR,
                    "Events Per Hour",
                    "Total events aggregated per hour.",
                    ChartType.LINE
            ),
            new KpiDefinition(
                    KpiId.EVENTS_PER_HOUR_BY_TYPE,
                    "Events Per Hour By Type",
                    "Stacked hourly event counts grouped by type.",
                    ChartType.STACKED_BAR
            ),
            new KpiDefinition(
                    KpiId.ERROR_RATE_PER_HOUR,
                    "Error Rate Per Hour",
                    "Hourly error percentage across events.",
                    ChartType.LINE
            ),
            new KpiDefinition(
                    KpiId.DURATION_STATS_PER_HOUR,
                    "Duration Stats Per Hour",
                    "Average and P95 duration metrics per hour.",
                    ChartType.LINE
            ),
            new KpiDefinition(
                    KpiId.SUCCESS_RATE,
                    "Success Rate",
                    "Overall percentage of successful events.",
                    ChartType.GAUGE
            ),
            new KpiDefinition(
                    KpiId.TOP_ACTORS,
                    "Top Actors",
                    "Top performing actors ranked by event count.",
                    ChartType.LEADERBOARD
            )
    );

    public List<KpiDefinition> listAll() {
        return definitions;
    }
}
