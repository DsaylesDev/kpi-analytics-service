package com.darion.kpi.kpis;

import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/kpis")
public class KpiController {

    private final KpiService service;
    private final KpiRegistry registry;
    private final KpiRequestNormalizer normalizer;

    public KpiController(KpiService service, KpiRegistry registry, KpiRequestNormalizer normalizer) {
        this.service = service;
        this.registry = registry;
        this.normalizer = normalizer;
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

        return service.getKpi(id, normalizer.normalize(req));
    }

    @GetMapping("/definitions")
    public List<KpiDefinition> definitions() {
        return registry.listAll();
    }
}
