/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.record.AvroBulletRecord;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

public class StrategyTest {
    private static class EmptyStrategy implements Strategy {
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
        public void reset() {
        }

        @Override
        public Optional<List<BulletError>> initialize() {
            return Optional.empty();
        }
    }
    @Test
    public void testDefaultClosed() {
        EmptyStrategy strategy = new EmptyStrategy();
        Assert.assertFalse(strategy.isClosed());
        strategy.consume(new AvroBulletRecord());
        Assert.assertFalse(strategy.isClosed());
    }
}
