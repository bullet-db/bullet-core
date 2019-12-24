/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import com.yahoo.bullet.result.JSONFormatter;
import lombok.Getter;

import java.util.Map;

/**
 * Simpler wrapper that represents a metric event being published.
 */
@Getter
public class MetricEvent implements JSONFormatter {
    private final String group;
    private final long timestamp;
    private final Map<String, String> dimensions;
    private final Map<String, Number> metrics;

    /**
     * Constructor.
     *
     * @param group The identity of the entity being measured by the metrics.
     * @param dimensions Static metadata describing the entity being measured.
     * @param metrics Numeric data describing values of the measured entity.
     */
    public MetricEvent(String group, Map<String, String> dimensions, Map<String, Number> metrics) {
        this.group = group;
        this.dimensions = dimensions;
        this.metrics = metrics;
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public String asJSON() {
        return JSONFormatter.asJSON(this);
    }
}

