/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Monoidal;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Meta;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * This represents the common parent for all windowing schemes. This wraps the {@link Strategy} since it needs to
 * control when the aggregation strategy resets its result etc. Windowing schemes generally accept
 * ({@link #consume(BulletRecord)} or {@link #combine(byte[])} data even if they are {@link #isClosed()} or
 * {@link #isClosedForPartition()}.
 */
@Slf4j
public abstract class Scheme implements Monoidal {
    protected Strategy aggregation;
    protected Window window;
    protected BulletConfig config;
    protected Map<String, String> metadataKeys;

    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy}, {@link Window} and
     * {@link BulletConfig}.
     *
     * @param aggregation The non-null initialized aggregation strategy that this window will operate.
     * @param window The initialized, configured window to use.
     * @param config The validated config to use.
     */
    public Scheme(Strategy aggregation, Window window, BulletConfig config) {
        this.aggregation = aggregation;
        this.window = window;
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
     * Returns true if this window is closed when operating in partition mode. If this window has been consuming slices
     * of the data (partitions) instead of the full data, this can be used to determine whether the window could or
     * needs to emit data to maintain the windowing invariant.
     *
     * @return A boolean whether this window is considered closed if it were consuming a slice of the data.
     */
    public abstract boolean isClosedForPartition();

    /**
     * Return any {@link Meta} for this windowing scheme and the {@link Strategy}.
     *
     * @return A non-null Meta object.
     */
    @Override
    public Meta getMetadata() {
        Meta meta = new Meta();
        boolean shouldMeta = config.getAs(BulletConfig.RESULT_METADATA_ENABLE, Boolean.class);
        if (shouldMeta) {
            String key = getMetaKey();
            if (key != null) {
                meta.add(key, getMetadata(metadataKeys));
            }
            meta.merge(aggregation.getMetadata());
        }
        return meta;
    }

    private String getMetaKey() {
        return metadataKeys.getOrDefault(Meta.Concept.WINDOW_METADATA.getName(), null);
    }
}
