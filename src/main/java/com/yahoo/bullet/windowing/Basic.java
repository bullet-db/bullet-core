/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.querying.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.result.Meta.addIfNonNull;

/**
 * This is a scheme that does not do any windowing. It just proxies the {@link com.yahoo.bullet.common.Monoidal} calls
 * to the {@link Strategy}. This window is only ever closed when the underlying {@link Strategy} is also
 * {@link Strategy#isClosed()}.
 */
public class Basic extends Scheme {
    public static final String NAME = "Windowless";
    protected long windowCount = 1L;

    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy} and {@link BulletConfig}.
     *
     * @param aggregation The non-null initialized aggregation strategy that this window will operate.
     * @param window The initialized, configured window to use.
     * @param config The validated config to use.
     */
    public Basic(Strategy aggregation, Window window, BulletConfig config) {
        super(aggregation, window, config);
    }

    @Override
    protected Map<String, Object> getMetadata(Map<String, String> metadataKeys) {
        Map<String, Object> meta = new HashMap<>();
        addIfNonNull(meta, metadataKeys, Meta.Concept.WINDOW_NAME, this::name);
        addIfNonNull(meta, metadataKeys, Meta.Concept.WINDOW_NUMBER, this::count);
        addIfNonNull(meta, metadataKeys, Meta.Concept.WINDOW_EMIT_TIME, System::currentTimeMillis);
        return meta;
    }

    /**
     * This consumes any data fed to it even if it is {@link #isClosed()} or {@link #isClosedForPartition()}.
     *
     * @param data The {@link BulletRecord} to consume.
     */
    @Override
    public void consume(BulletRecord data) {
        aggregation.consume(data);
    }

    /**
     * This combines any data fed to it even if it is {@link #isClosed()} or {@link #isClosedForPartition()}.
     *
     * @param data The {@link BulletRecord} to consume.
     */
    @Override
    public void combine(byte[] data) {
        aggregation.combine(data);
    }

    @Override
    public byte[] getData() {
        return aggregation.getData();
    }

    @Override
    public Clip getResult() {
        // This has already called aggregation.getMetadata
        Clip clip = Clip.of(getMetadata());
        clip.add(aggregation.getRecords());
        return clip;
    }

    @Override
    public List<BulletRecord> getRecords() {
        return aggregation.getRecords();
    }

    @Override
    public void reset() {
        aggregation.reset();
        windowCount++;
    }

    @Override
    public void resetForPartition() {
        reset();
    }

    @Override
    public boolean isClosed() {
        return aggregation.isClosed();
    }

    @Override
    public boolean isClosedForPartition() {
        return aggregation.isClosed();
    }

    @Override
    public void start(long startTime) {
    }

    /**
     * Gets the name of this windowing scheme.
     *
     * @return A String name for this window.
     */
    protected String name() {
        return NAME;
    }

    /**
     * Counts the number of windows that have been opened since creation.
     *
     * @return A long representing the number of windows opened.
     */
    protected long count() {
        // This should always be one unless reset is called (which it shouldn't because this window doesn't close).
        return windowCount;
    }
}
