/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

public class StrategyTest {
    class TestStrategy implements Strategy {
        @Override
        public void consume(BulletRecord data) {
        }

        @Override
        public void combine(byte[] data) {
        }

        @Override
        public byte[] getData() {
            return new byte[0];
        }

        @Override
        public Clip getResult() {
            return null;
        }

        @Override
        public List<BulletRecord> getRecords() {
            return null;
        }

        @Override
        public Optional<List<BulletError>> initialize() {
            return Optional.empty();
        }

        @Override
        public void reset() {
        }
    }

    @Test
    public void testDefaultAcceptance() {
        TestStrategy strategy = new TestStrategy();
        Assert.assertFalse(strategy.isClosed());
        strategy.consume(new BulletRecord());
        Assert.assertFalse(strategy.isClosed());
    }
}
