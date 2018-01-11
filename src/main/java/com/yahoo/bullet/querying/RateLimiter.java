/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import lombok.Getter;

/**
 * This class implements a simple rate checking mechanism. It accepts a maximum and a time window in which the maximum
 * should not be exceeded. Call {@link #increment()} for when what this is counting happens and then call
 * {@link #isRateLimited()} to check if the rate limit has been reached.
 */
@Getter
public class RateLimiter {
    private final int maximum;
    private final int timeInterval;
    private final double absoluteRateLimit;

    private long count = 0;
    private long lastCount = 0;
    private long lastCheckTime;

    public static final int SECOND = 1000;

    /**
     * Create an instance of this that uses a default time window of {@link #SECOND} ms.
     *
     * @param maximum A positive maximum count that this uses as the upper limit per the time interval.
     */
    public RateLimiter(int maximum) {
        this(maximum, SECOND);
    }

    /**
     * Create an instance of this that uses the given maximum and the given time interval.
     *
     * @param maximum A positive maximum count that is the limit for each time interval.
     * @param timeInterval The rate check will be done only at most once for this positive time interval in milliseconds.
     */
    public RateLimiter(int maximum, int timeInterval) {
        if (maximum <= 0 || timeInterval <= 0) {
            throw new IllegalArgumentException("Provide positive numbers for maximum and/or timeInterval");
        }
        this.maximum = maximum;
        this.timeInterval = timeInterval;
        this.absoluteRateLimit = maximum / (double) timeInterval;
        lastCheckTime = System.currentTimeMillis();
    }

    /**
     * Increment the measure that this is counting by one.
     */
    public void increment() {
        count++;
    }

    /**
     * Checks to see if this is rate limited. The rate is only checked at most once for each time interval. It is
     * therefore possible to exceed the absolute rate limit momentarily.
     *
     * @return A boolean denoting whether the rate limit has been exceeded.
     */
    public boolean isRateLimited() {
        long timeNow = System.currentTimeMillis();
        // If we are checking too early, do nothing
        if (Math.abs(timeNow - lastCheckTime) < timeInterval) {
            return false;
        }
        // It's time to check. Check if the count has exceeded the previous count, save it, update the last fields.
        boolean rateExceeded = currentRate(timeNow) > absoluteRateLimit;
        lastCount = count;
        lastCheckTime = timeNow;
        return rateExceeded;
    }

    /**
     * Returns the absolute current rate since the last check interval.
     *
     * @return A double representing the current absolute rate (per ms).
     */
    public double getCurrentRate() {
        return currentRate(System.currentTimeMillis());
    }

    private double currentRate(long timeNow) {
        return (count - lastCount) / (double) (timeNow - lastCheckTime);
    }
}
