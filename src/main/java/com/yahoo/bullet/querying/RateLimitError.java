package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RateLimitError extends BulletError {
    public static final String ERROR_FORMAT = "Exceeded the configured rate limit for the query: %f emits per second";
    public static final String ERROR_FORMAT_WITH_RATE = ERROR_FORMAT + ". Saw %f emits per second";
    public static final String NARROW_FILTER = "Try using more filters to reduce the data";
    public static final String TIME_WINDOW = "Try using a time based window instead of a record based window";
    public static final List<String> RESOLUTIONS = Arrays.asList(NARROW_FILTER, TIME_WINDOW);

    /**
     * Creates an instance of this from a given {@link BulletConfig}.
     *
     * @param config The validated BulletConfig to use.
     */
    public RateLimitError(BulletConfig config) {
        super(String.format(ERROR_FORMAT, getRate(config)), RESOLUTIONS);
    }

    /**
     * Creates an instance of this from a given absolute exceeded rate and a {@link BulletConfig}.
     *
     * @param rate The exceeded rate that caused the error.
     * @param config The validated BulletConfig to use.
     */
    public RateLimitError(double rate, BulletConfig config) {
        super(String.format(ERROR_FORMAT_WITH_RATE, getRate(config), rate * RateLimiter.SECOND), RESOLUTIONS);
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
