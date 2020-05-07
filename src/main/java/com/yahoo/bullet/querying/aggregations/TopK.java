/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.aggregations;

import com.yahoo.bullet.querying.aggregations.sketches.FrequentItemsSketch;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.TopKAggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.Clip;
import com.yahoo.sketches.frequencies.ErrorType;

import java.util.List;
import java.util.Map;

public class TopK extends SketchingStrategy<FrequentItemsSketch> {
    public static final String NEW_NAME_FIELD = "newName";

    public static final String NO_FALSE_NEGATIVES = "NFN";
    public static final String NO_FALSE_POSITIVES = "NFP";

    public static final String THRESHOLD_FIELD = "threshold";

    private final Map<String, String> fieldsToNames;
    private final String name;

    /**
     * Constructor that requires an {@link Aggregation} and a {@link BulletConfig} configuration.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     * @param config The config that has relevant configs for this strategy.
     */
    @SuppressWarnings("unchecked")
    public TopK(TopKAggregation aggregation, BulletConfig config) {
        super(aggregation, config);

        String errorConfiguration = config.getAs(BulletConfig.TOP_K_AGGREGATION_SKETCH_ERROR_TYPE, String.class);

        ErrorType errorType = getErrorType(errorConfiguration);

        fieldsToNames = aggregation.getFieldsToNames();
        name = aggregation.getName();

        int maxMapSize = config.getAs(BulletConfig.TOP_K_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        Long threshold = aggregation.getThreshold();
        int size = aggregation.getSize();
        BulletRecordProvider provider = config.getBulletRecordProvider();
        sketch = threshold != null ? new FrequentItemsSketch(errorType, maxMapSize, threshold, size, provider) :
                                     new FrequentItemsSketch(errorType, maxMapSize, size, provider);
    }

    @Override
    public void consume(BulletRecord data) {
        sketch.update(composeField(data));
    }

    @Override
    public Clip getResult() {
        Clip result = super.getResult();
        result.getRecords().forEach(this::splitFields);
        return result;
    }

    @Override
    public List<BulletRecord> getRecords() {
        List<BulletRecord> records = super.getRecords();
        records.forEach(this::splitFields);
        return records;
    }

    private void splitFields(BulletRecord record) {
        String field = (String) record.typedGet(FrequentItemsSketch.ITEM_FIELD).getValue();
        Long count = (Long) record.typedGet(FrequentItemsSketch.COUNT_FIELD).getValue();
        record.remove(FrequentItemsSketch.ITEM_FIELD);
        record.remove(FrequentItemsSketch.COUNT_FIELD);
        List<String> values = decomposeField(field);
        for (int i = 0; i < fields.size(); ++i) {
            String originalField = fields.get(i);
            String fieldName = fieldsToNames.get(originalField);
            record.setString(Utilities.isEmpty(fieldName) ? originalField : fieldName, values.get(i));
        }
        record.setLong(name, count);
    }

    private static ErrorType getErrorType(String errorType) {
        if (NO_FALSE_POSITIVES.equals(errorType)) {
            return ErrorType.NO_FALSE_POSITIVES;
        }
        return ErrorType.NO_FALSE_NEGATIVES;
    }
}
