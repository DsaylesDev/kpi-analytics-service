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
}

