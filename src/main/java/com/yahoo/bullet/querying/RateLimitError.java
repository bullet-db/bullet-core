/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.result.Meta;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RateLimitError extends BulletError {
    private static final long serialVersionUID = 5056840730518175058L;

    public static final String ERROR_FORMAT = "Exceeded the maximum rate limit for the query: %.2f emits per second. " +
                                              "Current rate: %.2f emits per second";
    public static final String NARROW_FILTER = "Try using more filters to reduce the data";
    public static final String TIME_WINDOW = "Try using a time based window instead of a record based window";
    public static final List<String> RESOLUTIONS = Arrays.asList(NARROW_FILTER, TIME_WINDOW);

    /**
    /**
     * Creates an instance of this from a given absolute exceeded rate and a {@link BulletConfig}.
     *
     * @param rate The exceeded rate that caused the error.
     * @param config The validated BulletConfig to use.
     */
    public RateLimitError(double rate, BulletConfig config) {
        super(String.format(ERROR_FORMAT, getRate(config), rate * RateLimiter.SECOND), RESOLUTIONS);
    }

    /**
     * Makes this into a {@link Meta} object.
     *
     * @return A meta object containing this error.
     */
    public Meta makeMeta() {
        return new Meta().addErrors(Collections.singletonList(this));
    }

    private static double getRate(BulletConfig config) {
        int maxCount = config.getAs(BulletConfig.RATE_LIMIT_MAX_EMIT_COUNT, Integer.class);
        int interval = config.getAs(BulletConfig.RATE_LIMIT_TIME_INTERVAL, Integer.class);
        return (maxCount / (double) interval) * RateLimiter.SECOND;
    }
}
