/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RateLimiterTest {
    private static void assertRateEquals(RateLimiter limiter, double rate) {
        try {
            // Sleep for 1 ms to make sure we don't get a NaN
            Thread.sleep(1);
            Assert.assertEquals(limiter.getCurrentRate(), rate);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testZeroMaximum() {
        new RateLimiter(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeMaximum() {
        new RateLimiter(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testZeroInterval() {
        new RateLimiter(10, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeInterval() {
        new RateLimiter(10, -10);
    }

    @Test
    public void testCreationWithDefaultTimeInterval() {
        RateLimiter limiter = new RateLimiter(10);
        Assert.assertEquals(limiter.getMaximum(), 10);
        Assert.assertEquals(limiter.getTimeInterval(), RateLimiter.SECOND);
    }

    @Test
    public void testCustomTimeInterval() {
        RateLimiter limiter = new RateLimiter(10, Integer.MAX_VALUE);
        Assert.assertEquals(limiter.getMaximum(), 10);
        Assert.assertEquals(limiter.getTimeInterval(), Integer.MAX_VALUE);
        Assert.assertFalse(limiter.isRateLimited());

        assertRateEquals(limiter, 0.0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAddingNegativeNumbers() {
        RateLimiter limiter = new RateLimiter(10, 1);
        limiter.add(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAddingZero() {
        RateLimiter limiter = new RateLimiter(10, 1);
        limiter.add(0);
    }

    @Test
    public void testExceedingRateLimit() throws Exception {
        RateLimiter limiter = new RateLimiter(10, 1);
        Assert.assertEquals(limiter.getMaximum(), 10);
        Assert.assertEquals(limiter.getTimeInterval(), 1);
        Assert.assertFalse(limiter.isRateLimited());

        // Force the rate limit being exceeded.
        limiter.add(Integer.MAX_VALUE);

        // Sleep for 1 ms to make sure we are over the time interval
        Thread.sleep(1);
        // count is now 100 and our sleep should have exceeded the time interval
        Assert.assertTrue(limiter.isRateLimited());

        assertRateEquals(limiter, 0.0);
    }

    @Test
    public void testCheckingTooEarly() {
        RateLimiter limiter = new RateLimiter(10, Integer.MAX_VALUE);
        Assert.assertEquals(limiter.getMaximum(), 10);
        Assert.assertEquals(limiter.getTimeInterval(), Integer.MAX_VALUE);

        Assert.assertFalse(limiter.isRateLimited());

        limiter.increment();
        Assert.assertFalse(limiter.isRateLimited());

        limiter.add(Integer.MAX_VALUE);
        Assert.assertFalse(limiter.isRateLimited());

        limiter.add(Integer.MAX_VALUE);
        Assert.assertFalse(limiter.isRateLimited());
    }
}
