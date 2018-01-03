/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Closable;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.result.Metadata;

import java.util.Collections;
import java.util.Map;

/**
 * This represents the common parent for all windowing schemes.
 */
public abstract class Scheme implements Initializable, Closable {
    private Strategy strategy;
    private BulletConfig config;
    private Map<String, String> metadataKeys;

    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy} and {@link BulletConfig}.
     *
     * @param strategy The non-null initialized aggregation strategy that this window will operate.
     * @param config The validated config to use.
     */
    public Scheme(Strategy strategy, BulletConfig config) {
        this.strategy = strategy;
        this.config = config;
        metadataKeys = (Map<String, String>) config.getAs(BulletConfig.RESULT_METADATA_METRICS, Map.class);
    }

    /**
     * Returns true if this window is closed.
     *
     * @return A boolean denoting whether this window is currently closed.
     */
    @Override
    public abstract boolean isClosed();


    /**
     * Return any {@link Metadata} for this windowing scheme and the {@link Strategy}.
     *
     * @return A non-null Metadata object.
     */
    public Metadata getMetadata() {
        Metadata metadata = new Metadata();
        boolean shouldMeta = config.getAs(BulletConfig.RESULT_METADATA_ENABLE, Boolean.class);
        if (shouldMeta) {
            String key = getMetaKey();
            if (key != null) {
                metadata.add(key, getMetadata(metadataKeys));
            }
            metadata.merge(strategy.getMetadata());
        }
        return metadata;
    }


    /**
     * Provide a {@link Runnable} to run everytime when the window is closed.
     *
     * @param runnable A Runnable object.
     */
    public abstract void onClose(Runnable runnable);

    /**
     * Return any metadata for the windowing scheme with the given configured names for the metadata concepts.
     *
     * @param metadataKeys The mapping of metadata concepts to their names to get metadata for.
     * @return A {@link Map} of strings to objects of the metadata.
     */
    protected abstract Map<String, Object> getMetadata(Map<String, String> metadataKeys);

    private String getMetaKey() {
        return metadataKeys.getOrDefault(Metadata.Concept.WINDOW_METADATA.getName(), null);
    }
}
