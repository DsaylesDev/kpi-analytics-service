package com.darion.kpi.events;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface WarehouseEventRepository extends ElasticsearchRepository<WarehouseEvent, String> {
}
