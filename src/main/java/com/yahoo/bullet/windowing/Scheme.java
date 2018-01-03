/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Queryable;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Metadata;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * This represents the common parent for all windowing schemes. This wraps the {@link Strategy} since it needs to
 * control when the aggregation strategy resets its result etc.
 */
@Slf4j
public abstract class Scheme implements Queryable {
    private Strategy aggregation;
    private BulletConfig config;
    private Map<String, String> metadataKeys;

    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy} and {@link BulletConfig}.
     *
     * @param aggregation The non-null initialized aggregation strategy that this window will operate.
     * @param config The validated config to use.
     */
    public Scheme(Strategy aggregation, BulletConfig config) {
        this.aggregation = aggregation;
        this.config = config;
        metadataKeys = (Map<String, String>) config.getAs(BulletConfig.RESULT_METADATA_METRICS, Map.class);
    }

    /**
     * Return any metadata for the windowing scheme with the given configured names for the metadata concepts.
     *
     * @param metadataKeys The mapping of metadata concepts to their names to get metadata for.
     * @return A {@link Map} of strings to objects of the metadata.
     */
    protected abstract Map<String, Object> getMetadata(Map<String, String> metadataKeys);

    /**
     * Return any {@link Metadata} for this windowing scheme and the {@link Strategy}.
     *
     * @return A non-null Metadata object.
     */
    @Override
    public Metadata getMetadata() {
        Metadata metadata = new Metadata();
        boolean shouldMeta = config.getAs(BulletConfig.RESULT_METADATA_ENABLE, Boolean.class);
        if (shouldMeta) {
            String key = getMetaKey();
            if (key != null) {
                metadata.add(key, getMetadata(metadataKeys));
            }
            metadata.merge(aggregation.getMetadata());
        }
        return metadata;
    }

    private String getMetaKey() {
        return metadataKeys.getOrDefault(Metadata.Concept.WINDOW_METADATA.getName(), null);
    }

    /**
     * Unconditionally consume a {@link BulletRecord} into the aggregation. Use this method once the windowing
     * criteria is met.
     *
     * @param record The record to consume.
     */
    protected void aggregate(BulletRecord record) {
        try {
            aggregation.consume(record);
        } catch (RuntimeException e) {
            log.error("Unable to consume {} for query {}", record, this);
            log.error("Skipping due to", e);
        }
    }
}
