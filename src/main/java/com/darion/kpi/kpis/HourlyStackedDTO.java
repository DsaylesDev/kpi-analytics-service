package com.darion.kpi.kpis;

import java.util.List;

public record HourlyStackedDTO(String hour, long total, List<DonutSliceDTO> byType) {}
