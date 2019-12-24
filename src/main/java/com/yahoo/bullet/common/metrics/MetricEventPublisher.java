/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import java.util.Map;

/**
 * This class concretizes {@link MetricPublisher} with {@link MetricEvent}.
 */
public interface MetricEventPublisher extends MetricPublisher<MetricEvent> {
    String DEFAULT_GROUP = "default";

    /**
     * Get the group to use for the {@link MetricEvent}. By default, uses {@link #DEFAULT_GROUP}.
     *
     * @return The group to use for the {@link MetricEvent}.
     */
    default String getGroup() {
        return DEFAULT_GROUP;
    }

    @Override
    default MetricEvent convert(Map<String, String> dimensions, Map<String, Number> metrics) {
        return new MetricEvent(getGroup(), dimensions, metrics);
    }
}
