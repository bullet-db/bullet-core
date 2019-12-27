/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import com.yahoo.bullet.common.BulletConfig;

import java.util.Map;

/**
 * This class concretizes {@link MetricPublisher} with {@link MetricEvent}.
 */
public abstract class MetricEventPublisher extends MetricPublisher<MetricEvent> {
    public static String DEFAULT_GROUP = "default";

    /**
     * Constructor taking a {@link BulletConfig}.
     *
     * @param config The config to use containing the necessary information.
     */
    public MetricEventPublisher(BulletConfig config) {
        super(config);
    }

    /**
     * Get the group to use for the {@link MetricEvent}. By default, uses {@link #DEFAULT_GROUP}.
     *
     * @return The group to use for the {@link MetricEvent}.
     */
    public String getGroup() {
        return DEFAULT_GROUP;
    }

    @Override
    public MetricEvent convert(Map<String, String> dimensions, Map<String, Number> metrics) {
        return new MetricEvent(getGroup(), dimensions, metrics);
    }
}
