/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.aggregations.grouping.GroupData;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.aggregations.sketches.TupleSketch;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.GroupAggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.sketches.ResizeFactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This {@link Strategy} implements a Tuple Sketch based approach to doing a group by. In particular, it
 * provides a uniform sample of the groups if the number of unique groups exceed the Sketch size. Metrics like
 * sum and count when summed across the uniform sample and divided the sketch theta gives an approximate estimate
 * of the total sum and count across all the groups.
 */
public class GroupBy extends KMVStrategy<TupleSketch> {
    // This is reused for the duration of the strategy.
    private final CachingGroupData container;

    /**
     * Constructor that requires an {@link Aggregation} and a {@link BulletConfig} configuration.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     * @param config The config that has relevant configs for this strategy.
     */
    @SuppressWarnings("unchecked")
    public GroupBy(GroupAggregation aggregation, BulletConfig config) {
        super(aggregation, config);

        Map<GroupOperation, Number> metrics = GroupData.makeInitialMetrics(aggregation.getOperations());
        container = new CachingGroupData(null, aggregation.getFieldsToNames(), metrics);

        ResizeFactor resizeFactor = getResizeFactor(config, BulletConfig.GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR);
        float samplingProbability = config.getAs(BulletConfig.GROUP_AGGREGATION_SKETCH_SAMPLING, Float.class);

        // Default at 512 gives a 13.27% error rate at 99.73% confidence (3 SD). Irrelevant since we are using this to
        // mostly cap the number of groups. You can use the Sketch theta to extrapolate the aggregation for all the data.
        int nominalEntries = config.getAs(BulletConfig.GROUP_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        int maximumSize = config.getAs(BulletConfig.GROUP_AGGREGATION_MAX_SIZE, Integer.class);
        int size = Math.min(aggregation.getSize(), maximumSize);

        sketch = new TupleSketch(resizeFactor, samplingProbability, nominalEntries, size, config.getBulletRecordProvider());
    }

    @Override
    public void consume(BulletRecord data) {
        Map<String, String> fieldToValues = getFields(data);
        // More optimal than calling composeFields
        String key = getFieldsAsString(fields, fieldToValues);

        // Set the record and the group values into the container. The metrics are already initialized.
        container.setCachedRecord(data);
        container.setGroupFields(fieldToValues);
        sketch.update(key, container);
    }

    private Map<String, String> getFields(BulletRecord record) {
        Map<String, String> fieldValues = new HashMap<>();
        for (String field : fields) {
            String value = Objects.toString(record.typedGet(field).getValue());
            fieldValues.put(field, value);
        }
        return fieldValues;
    }

    private String getFieldsAsString(List<String> fields, Map<String, String> mapping) {
        return composeField(fields.stream().map(mapping::get));
    }
}
