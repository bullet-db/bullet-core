/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import com.yahoo.bullet.common.BulletConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class MetricPublisher<T> implements AutoCloseable {
    protected BulletConfig config;

    /**
     * Constructor taking a {@link BulletConfig}.
     *
     * @param config The config to use containing the necessary information.
     */
    public MetricPublisher(BulletConfig config) {
        this.config = config;
    }

    /**
     * Returns any static dimensions to use. These are added to any additional dimensions provided.
     *
     * @return The {@link Map} of static and extra dimensions to use for all publishing.
     */
    public Map<String, String> getDimensions(Map<String, String> extraDimensions) {
        return new HashMap<>(extraDimensions);
    }

    /**
     * Fire and forget an event with no extra dimensions and the given single metric.
     *
     * @param metricName The name of the metric being published.
     * @param metricValue The value of the metric being published.
     */
    public void fire(String metricName, Number metricValue) {
        fire(Collections.emptyMap(), Collections.singletonMap(metricName, metricValue));
    }

    /**
     * Fire and forget an event with no extra dimensions and the given metrics.
     *
     * @param metrics The metrics to publish.
     */
    public void fire(Map<String, Number> metrics) {
        fire(Collections.emptyMap(), metrics);
    }

    /**
     * Fire and forget an event with the given extra dimensions and the given metrics.
     *
     * @param dimensions The additional dimensions to publish.
     * @param metrics The metrics to publish.
     */
    public void fire(Map<String, String> dimensions, Map<String, Number> metrics) {
        if (metrics.isEmpty()) {
            return;
        }
        publish(dimensions, metrics);
    }

    /**
     * Publishes an event with no extra dimensions and the given single metric.
     *
     * @param metricName The name of the metric being published.
     * @param metricValue The value of the metric being published.
     * @return A {@link CompletableFuture} that resolves to true or false depending on whether the publish succeeded.
     */
    public CompletableFuture<Boolean> publish(String metricName, Number metricValue) {
        return publish(Collections.singletonMap(metricName, metricValue));
    }

    /**
     * Publishes an event no extra dimensions and the given metrics.
     *
     * @param metrics The metrics to publish.
     * @return A {@link CompletableFuture} that resolves to true or false depending on whether the publish succeeded.
     */
    public CompletableFuture<Boolean> publish(Map<String, Number> metrics) {
        return publish(Collections.emptyMap(), metrics);
    }

    /**
     * Publish an event with the given dimensions and metrics.
     *
     * @param dimensions The non-null additional dimensions to publish.
     * @param metrics The metrics to publish.
     * @return A {@link CompletableFuture} that resolves to true or false depending on whether the publish succeeded.
     */
    public CompletableFuture<Boolean> publish(Map<String, String> dimensions, Map<String, Number> metrics) {
        if (metrics.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        return publish(convert(getDimensions(dimensions), metrics));
    }

    /**
     * Convert the given dimensions and metrics into the concrete type of this class.
     *
     * @param dimensions The non-null {@link Map} of dimensions to convert.
     * @param metrics The {@link Map} of metrics to convert.
     * @return The concrete type object of this class to publish.
     */
    public abstract T convert(Map<String, String> dimensions, Map<String, Number> metrics);

    /**
     * Publish a payload of the concrete type of this class.
     *
     * @param payload The payload to publish.
     * @return A {@link CompletableFuture} that resolves to true or false depending on whether the publish succeeded.
     */
    public abstract CompletableFuture<Boolean> publish(T payload);

    /**
     * Create a {@link MetricPublisher} instance using the class specified in the config file.
     *
     * @param config The non-null {@link BulletConfig} containing the class name and metric publisher settings.
     * @return an instance of specified class initialized with settings from the input file and defaults.
     */
    public static MetricPublisher from(BulletConfig config) {
        try {
            return config.loadConfiguredClass(BulletConfig.METRIC_PUBLISHER_CLASS_NAME);
        } catch (RuntimeException e) {
            throw new RuntimeException("Cannot create MetricPublisher instance.", e.getCause());
        }
    }
}

