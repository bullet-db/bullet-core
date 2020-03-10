/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Utility class to concurrently store frequency and average metrics for various string keys. Averages are computed by
 * storing the absolute totals of each metric along with the counts and the actual average division is computed on
 * {@link #extractMetrics()}. This method will also reset all the metrics stored. Use this class if you want to count
 * or average metrics periodically and with high concurrency, extract them and report them somewhere.
 */
@NoArgsConstructor @Slf4j
public class MetricCollector {
    private final ConcurrentMap<String, Number> metrics = new ConcurrentHashMap<>();
    private final Set<String> averageMetrics = new HashSet<>();

    private static final String SUM = ".s";
    private static final String FREQUENCY = ".f";

    /**
     * A constructor that sets up the collector with an initial count of 0 for each of the given metrics.
     *
     * @param frequencies The {@link List} of metrics to initialize to a count of zero.
     */
    public MetricCollector(List<String> frequencies) {
        this(frequencies, Collections.emptyList());
    }

    /**
     * A constructor that sets up the collector with an initial count of 0 for each of the given metrics.
     *
     * @param frequencies The {@link List} of frequencies to initialize to a count of zero.
     * @param averages The {@link List} of averages to maintain.
     */
    public MetricCollector(List<String> frequencies, List<String> averages) {
        frequencies.forEach(m -> add(m, 0L));
        averageMetrics.addAll(averages);
    }

    /**
     * Increment the value for the given key. It will be created if it does not exist.
     *
     * @param key The key to increment.
     */
    public void increment(String key) {
        add(key, 1L);
    }

    /**
     * Add the given total to the existing sum for the given key. It will be created if it does not exist.
     *
     * @param key The key to add to.
     * @param total The total to add as.
     */
    public void add(String key, long total) {
        LongAdder count = (LongAdder) metrics.computeIfAbsent(key, k -> new LongAdder());
        count.add(total);
        log.debug("Incrementing metric for {} to {}", key, count);
    }

    /**
     * Increment one instance of an average metric with the absolute total.
     *
     * @param key The average metric to add the total to.
     * @param absoluteTotal The total to add.
     */
    public void average(String key, long absoluteTotal) {
        average(key, absoluteTotal, 1L);
    }

    /**
     * Average the given absolute total and frequency total into the metric so far.
     *
     * @param key The name of the average metric.
     * @param absoluteTotal The total of the metric being averaged.
     * @param frequencyTotal The total number of instances of the absolute metric provided in this call.
     */
    public void average(String key, long absoluteTotal, long frequencyTotal) {
        add(sum(key), absoluteTotal);
        add(frequency(key), frequencyTotal);
    }

    /**
     * Gets and resets the metrics currently stored in the collector.
     *
     * @return {@link Map} of names to counts of frequencies.
     */
    public Map<String, Number> extractMetrics() {
        final Map<String, Number> current = new HashMap<>();
        metrics.forEach((k, v) -> current.put(k, ((LongAdder) v).sumThenReset()));
        averageMetrics.forEach(m -> computeAverage(m, current));
        return current;
    }

    private void computeAverage(String name, Map<String, Number> metrics) {
        Number sum = metrics.remove(sum(name));
        Number total = metrics.remove(frequency(name));
        // Sufficient to check null for just sum or total
        double average = 0;
        if (total != null) {
            average = sum.doubleValue() / total.longValue();
        }
        average = Double.isFinite(average) ? average : 0;
        log.debug("Average for {} is {}", name, average);
        metrics.put(name, average);
    }

    private String sum(String key) {
        return key + SUM;
    }

    private String frequency(String key) {
        return key + FREQUENCY;
    }
}
