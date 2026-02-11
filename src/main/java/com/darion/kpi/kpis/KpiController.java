package com.darion.kpi.kpis;

import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@RestController
@RequestMapping("/kpis")
public class KpiController {

    private final KpiService service;

    public KpiController(KpiService service) {
        this.service = service;
    }

    @GetMapping("/{id}")
    public Object getKpi(
            @PathVariable KpiId id,
            @RequestParam String from,
            @RequestParam String to,
            @RequestParam(required = false) String siteId,
            @RequestParam(required = false) Integer topN,
            @RequestParam(required = false) Integer limit
    ) {
        KpiRequest req = new KpiRequest(
                Instant.parse(from),
                Instant.parse(to),
                siteId,
                topN,
                limit
        );
        return service.getKpi(id, req);
    }
}
