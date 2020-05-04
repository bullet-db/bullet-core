/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import lombok.Getter;

import java.util.List;

@Getter
public class MockStrategy implements Strategy {
    private int consumeCalls = 0;
    private int combineCalls = 0;
    private int dataCalls = 0;
    private int resultCalls = 0;
    private int recordCalls = 0;
    private int metadataCalls = 0;
    private int resetCalls = 0;
    private int closedCalls = 0;

    @Override
    public void consume(BulletRecord data) {
        consumeCalls++;
    }

    @Override
    public void combine(byte[] data) {
        combineCalls++;
    }

    @Override
    public byte[] getData() {
        dataCalls++;
        return null;
    }

    @Override
    public Clip getResult() {
        resultCalls++;
        return null;
    }

    @Override
    public List<BulletRecord> getRecords() {
        recordCalls++;
        return null;
    }

    @Override
    public void reset() {
        resetCalls++;
    }

    @Override
    public boolean isClosed() {
        closedCalls++;
        return false;
    }

    @Override
    public Meta getMetadata() {
        metadataCalls++;
        return null;
    }
}
