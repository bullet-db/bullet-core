/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Window;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;
import static java.util.Collections.singletonList;

/**
 * This window is just a {@link SlidingRecord} window with a maximum of one record - both {@link #isClosedForPartition()}
 * and {@link #isClosed()} are identical. Using this will make results be emitted as they arrive - making it snappy or
 * reactive.
 */
public class Reactive extends SlidingRecord {
    public static final int SINGLE_RECORD = 1;

    public static final BulletError ONLY_ONE_RECORD = makeError("The \"every\" field had bad values for \"RECORD\"",
                                                                "Please set \"every\" to 1");

    /**
     * Creates an instance of this windowing scheme with the provided {@link Strategy} and {@link BulletConfig}.
     *
     * @param aggregation The non-null initialized aggregation strategy that this window will operate.
     * @param window The initialized, configured window to use.
     * @param config The validated config to use.
     */
    public Reactive(Strategy aggregation, Window window, BulletConfig config) {
        super(aggregation, window, config);
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        // Do super first to initialize maxCount
        Optional<List<BulletError>> errors = super.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (maxCount != SINGLE_RECORD) {
            return Optional.of(singletonList(ONLY_ONE_RECORD));
        }
        return Optional.empty();
    }
}
