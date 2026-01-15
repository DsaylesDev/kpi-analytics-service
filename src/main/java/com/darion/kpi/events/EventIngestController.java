package com.darion.kpi.events;

import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@RestController
@RequestMapping("/events")
public class EventIngestController {
    private final WarehouseEventRepository repo;

    public EventIngestController(WarehouseEventRepository repo) {
        this.repo = repo;
    }
    @PostMapping
    public WarehouseEvent ingest(@RequestBody WarehouseEventRequest req) {
        WarehouseEvent event = new WarehouseEvent();
        event.setTimestamp(Instant.parse(req.timestamp())); // <-- key fix
        event.setEventType(req.eventType());
        event.setSessionId(req.sessionId());
        event.setActorId(req.actorId());
        event.setSiteId(req.siteId());
        event.setDurationMs(req.durationMs());
        event.setSuccess(req.success());

        return repo.save(event);
    }
}
