package com.darion.kpi.kpis;

import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/kpis")
public class KpiController {

    private final KpiService service;

    public KpiController(KpiService service) {
        this.service = service;
    }

    @GetMapping("/event-type-breakdown")
    public List<DonutSliceDTO> eventTypeBreakdown(
            @RequestParam String from,
            @RequestParam String to,
            @RequestParam(required = false) String siteId
    ) {
        return service.eventTypeBreakdown(Instant.parse(from), Instant.parse(to), siteId);
    }
    @GetMapping("/events-per-hour")
    public List<HourlyCountDTO> eventsPerHour(
            @RequestParam String from,
            @RequestParam String to,
            @RequestParam(required = false) String siteId
    ) {
        return service.eventsPerHour(Instant.parse(from), Instant.parse(to), siteId);
    }
    @GetMapping("/success-rate")
    public SuccessRateDTO successRate(
            @RequestParam String from,
            @RequestParam String to,
            @RequestParam(required = false) String siteId
    ) {
        return service.successRate(Instant.parse(from), Instant.parse(to), siteId);
    }
    @GetMapping("/leaderboard/top-actors")
    public List<LeaderboardEntryDTO> topActors(
            @RequestParam String from,
            @RequestParam String to,
            @RequestParam(required = false) String siteId,
            @RequestParam(defaultValue = "10") int limit
    ) {
        return service.topActors(
                Instant.parse(from),
                Instant.parse(to),
                siteId,
                limit
        );
    }
    @GetMapping("/events-per-hour/by-type")
    public List<HourlyStackedDTO> eventsPerHourByType(
            @RequestParam String from,
            @RequestParam String to,
            @RequestParam(required = false) String siteId
    ) {
        return service.eventsPerHourByType(Instant.parse(from), Instant.parse(to), siteId);
    }


}

