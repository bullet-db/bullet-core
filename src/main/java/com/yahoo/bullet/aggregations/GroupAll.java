/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.aggregations.grouping.GroupData;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singletonList;

@Slf4j
public class GroupAll implements Strategy {
    // We only have a single group.
    private GroupData data;

    private Set<GroupOperation> operations;
    /**
     * Constructor that requires an {@link Aggregation} and a {@link BulletConfig} configuration.
     *
     * @param aggregation The {@link Aggregation} that specifies how and what this will compute.
     * @param config The config that has relevant configs for this strategy. It is unused currently.
     */
    public GroupAll(Aggregation aggregation, BulletConfig config) {
        // GroupOperations is all we care about - size etc. are meaningless for Group All since it's a single result
        operations = GroupOperation.getOperations(aggregation.getAttributes());
        data = new GroupData(operations);
    }

    @Override
    public Optional<List<Error>> initialize() {
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
    public void combine(byte[] serializedAggregation) {
        data.combine(serializedAggregation);
    }

    @Override
    public byte[] getSerializedAggregation() {
        return SerializerDeserializer.toBytes(data);
    }

    @Override
    public Clip getAggregation() {
        return Clip.of(data.getMetricsAsBulletRecord());
    }
}
