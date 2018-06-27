/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.result.Meta;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static com.yahoo.bullet.common.BulletConfig.DEFAULT_RATE_LIMIT_MAX_EMIT_COUNT;
import static com.yahoo.bullet.common.BulletConfig.DEFAULT_RATE_LIMIT_TIME_INTERVAL;

public class RateLimitErrorTest {
    @Test
    public void testMetaAndRateConversion() {
        BulletConfig config = new BulletConfig();
        RateLimitError error = new RateLimitError(19.34, config);
        Map<String, Object> actual = error.makeMeta().asMap();
        Assert.assertTrue(actual.containsKey(Meta.ERROR_KEY));
        Assert.assertTrue(((List<BulletError>) actual.get(Meta.ERROR_KEY)).get(0) == error);

        String asJSON = error.asJSON();
        double defaultRate = ((double) DEFAULT_RATE_LIMIT_MAX_EMIT_COUNT / DEFAULT_RATE_LIMIT_TIME_INTERVAL) * RateLimiter.SECOND;
        double actualRate = 19.34 * RateLimiter.SECOND;
        assertJSONEquals(asJSON, "{'error': '" + String.format(RateLimitError.ERROR_FORMAT, defaultRate, actualRate) + "', " +
                                  "'resolutions': ['" + RateLimitError.NARROW_FILTER + "', '" + RateLimitError.TIME_WINDOW + "']" +
                                 "}");
    }
}
