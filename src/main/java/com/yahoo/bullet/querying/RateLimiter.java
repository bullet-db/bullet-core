/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import lombok.Getter;

/**
 * This class implements a simple rate checking mechanism. It accepts a maximum and a time window in which the maximum
 * should not be exceeded. Call {@link #increment()} (or {@link #add(int) if batching }for when what this is counting
 * happens and then call {@link #isRateLimited()} to check if the rate limit has been reached.
 *
 * You should check if the rate is limited at least once per your given time interval. If you check too late, your
 * counted measure will be <strong>averaged</strong> over the time duration since the last check, which may or may not
 * cause your rate limit criteria to be violated. This can happen if your increments or adds are bursty so you exceed
 * the rate limit within your time interval but you neglect to check it for a long enough time interval where the burst
 * is spread out over bringing the overall rate lower than your configured maximum and yielding a false negative.
 */
@Getter
public class RateLimiter {
    private final int maximum;
    private final int timeInterval;
    private final double absoluteRateLimit;

    private long count = 0;
    private long lastCount = 0;
    private long lastCheckTime;
    private boolean exceededRate = false;

    public static final int SECOND = 1000;

    /**
     * Create an instance of this that uses a default time window of {@link #SECOND} ms.
     *
     * @param maximum A positive maximum count that this uses as the upper limit per the time interval.
     * @throws IllegalArgumentException if the maximum was not positive.
     */
    public RateLimiter(int maximum) throws IllegalArgumentException {
        this(maximum, SECOND);
    }

    /**
     * Create an instance of this that uses the given maximum and the given time interval.
     *
     * @param maximum A positive maximum count that is the limit for each time interval.
     * @param timeInterval The rate check will be done only at most once for this positive time interval in milliseconds.
     * @throws IllegalArgumentException if the maximum or the time interval were not positive.
     */
    public RateLimiter(int maximum, int timeInterval) throws IllegalArgumentException {
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
     * Increment the measure that this is counting by the given positive number.
     *
     * @param n The number to add to the count.
     * @throws IllegalArgumentException if the given number was not positive.
     */
    public void add(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("Add only positive numbers!");
        }
        count += n;
    }

    /**
     * Checks to see if this is rate limited. The rate is only checked at most once for each time interval. It is
     * therefore possible to exceed the absolute rate limit momentarily.
     *
     * @return A boolean denoting whether the rate limit has been exceeded.
     */
    public boolean isRateLimited() {
        // Once exceeded, always exceeded.
        if (exceededRate) {
            return true;
        }
        long timeNow = System.currentTimeMillis();
        // Do nothing if too early
        if (isTooEarly(timeNow)) {
            return false;
        }
        // It's time to check. Check if the count has exceeded the previous count, if so, update the last fields.
        exceededRate = getCurrentRate(timeNow) > absoluteRateLimit;
        if (!exceededRate) {
            lastCount = count;
            lastCheckTime = timeNow;
        }
        return exceededRate;
    }

    /**
     * Returns the absolute current rate since the last check interval.
     *
     * @return A double representing the current absolute rate (per ms).
     */
    public double getCurrentRate() {
        return getCurrentRate(System.currentTimeMillis());
    }

    /**
     * Test helper to reset the count.
     */
    void resetCounts() {
        lastCount = 0;
        count = 0;
    }

    private boolean isTooEarly(long timeNow) {
        return Math.abs(timeNow - lastCheckTime) < timeInterval;
    }

    private double getCurrentRate(long timeNow) {
        // If denominator is zero, it will be NaN or Infinity
        return (count - lastCount) / (double) (timeNow - lastCheckTime);
    }

}
