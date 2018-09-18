/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.aggregations.grouping.GroupData;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.Clip;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singletonList;

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
    public GroupAll(Aggregation aggregation, BulletConfig config) {
        // GroupOperations is all we care about - size etc. are meaningless for Group All since it's a single result
        operations = GroupOperation.getOperations(aggregation.getAttributes());
        data = new GroupData(operations);
        this.provider = config.getBulletRecordProvider();
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (Utilities.isEmpty(operations)) {
            return Optional.of(singletonList(GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR));
        }
        return GroupOperation.checkOperations(operations);
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
