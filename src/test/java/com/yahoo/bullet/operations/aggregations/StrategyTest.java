/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class StrategyTest {
    class TestStrategy implements Strategy {
        @Override
        public void consume(BulletRecord data) {
        }

        @Override
        public void combine(byte[] serializedAggregation) {
        }

        @Override
        public byte[] getSerializedAggregation() {
            return new byte[0];
        }

        @Override
        public Clip getAggregation() {
            return null;
        }

        @Override
        public List<Error> initialize() {
            return null;
        }
    }

    @Test
    public void testDefaultAcceptance() {
        TestStrategy strategy = new TestStrategy();
        Assert.assertTrue(strategy.isAcceptingData());
        strategy.consume(new BulletRecord());
        Assert.assertTrue(strategy.isAcceptingData());
    }

    @Test
    public void testDefaultMicroBatching() {
        TestStrategy strategy = new TestStrategy();
        Assert.assertFalse(strategy.isMicroBatch());
    }
}
