/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.aggregations.grouping.GroupData;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.GroupAggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.Clip;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public class GroupAll implements Strategy {
    // We only have a single group.
    private GroupData data;

    private Set<GroupOperation> operations;
    private BulletRecordProvider provider;
    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation The {@link Aggregation} that specifies how and what this will compute.
     * @param config The BulletConfig.
     */
    public GroupAll(GroupAggregation aggregation, BulletConfig config) {
        // GroupOperations is all we care about - size etc. are meaningless for Group All since it's a single result
        operations = aggregation.getOperations();
        data = new GroupData(operations);
        this.provider = config.getBulletRecordProvider();
    }

    @Override
    public void consume(BulletRecord data) {
        this.data.consume(data);
    }

    @Override
    public void combine(byte[] data) {
        this.data.combine(data);
    }

    @Override
    public byte[] getData() {
        return SerializerDeserializer.toBytes(data);
    }

    @Override
    public Clip getResult() {
        return Clip.of(getRecords());
    }

    @Override
    public List<BulletRecord> getRecords() {
        List<BulletRecord> list = new ArrayList<>();
        list.add(data.getMetricsAsBulletRecord(provider));
        return list;
    }

    @Override
    public void reset() {
        data = new GroupData(operations);
    }
}
