/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.partitioning;

import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.record.BulletRecord;

import java.util.List;

public class MockPartitioner implements Partitioner {
    @Override
    public List<String> getKeys(Query query) {
        return null;
    }

    @Override
    public List<String> getKeys(BulletRecord record) {
        return null;
    }
}
