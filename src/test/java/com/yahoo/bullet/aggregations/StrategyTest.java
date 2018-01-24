/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.record.BulletRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StrategyTest {
    @Test
    public void testDefaultAcceptance() {
        MockStrategy strategy = new MockStrategy();
        Assert.assertFalse(strategy.isClosed());
        strategy.consume(new BulletRecord());
        Assert.assertFalse(strategy.isClosed());
    }
}
