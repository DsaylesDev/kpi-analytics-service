package com.darion.kpi.events;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/events")
public class EventIngestController {
    private final WarehouseEventRepository repo;

    public EventIngestController(WarehouseEventRepository repo) {
        this.repo = repo;
    }
    @PostMapping
    public WarehouseEvent ingest(@RequestBody WarehouseEvent event) {
        return repo.save(event);
    }
}
