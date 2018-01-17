/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.parsing.Window;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;
import static java.util.Collections.singletonList;

public class Tumbling extends Basic {
    public static final String NAME = "Tumbling";

    private long startedAt;
    private long closeAfter;

    public static final BulletError MISSING_EVERY = makeError("The \"every\" field was not found or had bad values",
                                                              "Please set \"every\" in \"emit\" to a proper number");
    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy} and {@link BulletConfig}.
     *
     * @param aggregation The non-null initialized aggregation strategy that this window will operate.
     * @param window The initialized, configured window to use.
     * @param config The validated config to use.
     */
    public Tumbling(Strategy aggregation, Window window, BulletConfig config) {
        super(aggregation, window, config);
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Number every = Utilities.getCasted(window.getEmit(), Window.EMIT_EVERY_FIELD, Number.class);
        if (every == null) {
            return Optional.of(singletonList(MISSING_EVERY));
        }
        closeAfter = every.longValue();
        startedAt = System.currentTimeMillis();
        return Optional.empty();
    }

    @Override
    public void reset() {
        super.reset();
        startedAt = System.currentTimeMillis();
    }

    @Override
    public boolean isClosed() {
        return super.isClosed() || System.currentTimeMillis() >= startedAt + closeAfter;
    }

    @Override
    public boolean isClosedForPartition() {
        // For tumbling windows, isClosedForPartition is the same as isClosed.
        return isClosed();
    }

    @Override
    protected String name() {
        return NAME;
    }
}
