/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupData;
import com.yahoo.bullet.operations.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.operations.aggregations.sketches.TupleSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.parsing.Specification;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.sketches.ResizeFactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This {@link Strategy} implements a Tuple Sketch based approach to doing a group by. In particular, it
 * provides a uniform sample of the groups if the number of unique groups exceed the Sketch size. Metrics like
 * sum and count when summed across the uniform sample and divided the sketch theta gives an approximate estimate
 * of the total sum and count across all the groups.
 */
public class GroupBy extends KMVStrategy<TupleSketch> {
    // This is reused for the duration of the strategy.
    private final CachingGroupData container;

    private final Set<GroupOperation> operations;

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public GroupBy(Aggregation aggregation) {
        super(aggregation);

        Map<String, Object> attributes = aggregation.getAttributes();
        operations = GroupOperation.getOperations(attributes);
        Map<GroupOperation, Number> metrics = GroupData.makeInitialMetrics(operations);
        container = new CachingGroupData(null, metrics);

        ResizeFactor resizeFactor = getResizeFactor(BulletConfig.GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR);
        float samplingProbability = config.getAs(BulletConfig.GROUP_AGGREGATION_SKETCH_SAMPLING, Float.class);

        // Default at 512 gives a 13.27% error rate at 99.73% confidence (3 SD). Irrelevant since we are using this to
        // mostly cap the number of groups. You can use the Sketch theta to extrapolate the aggregation for all the data.
        int nominalEntries = config.getAs(BulletConfig.GROUP_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        int size = aggregation.getSize();

        sketch = new TupleSketch(resizeFactor, samplingProbability, nominalEntries, size);
    }

    @Override
    public List<Error> initialize() {
        return GroupOperation.checkOperations(operations);
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

    @Override
    public Clip getAggregation() {
        Clip result = super.getAggregation();
        result.getRecords().forEach(this::renameFields);
        return result;
    }

    private Map<String, String> getFields(BulletRecord record) {
        Map<String, String> fieldValues = new HashMap<>();
        for (String field : fields) {
            // This explicitly does not do a TypedObject checking. Nulls (and everything else) turn into Strings
            String value = Objects.toString(Specification.extractField(field, record));
            fieldValues.put(field, value);
        }
        return fieldValues;
    }

    private void renameFields(BulletRecord record) {
        for (Map.Entry<String, String> entry : fieldsToNames.entrySet()) {
            String newName = entry.getValue();
            if (!Utilities.isEmpty(newName)) {
                record.rename(entry.getKey(), newName);
            }
        }
    }

    private String getFieldsAsString(List<String> fields, Map<String, String> mapping) {
        return composeField(fields.stream().map(mapping::get));
    }
}
