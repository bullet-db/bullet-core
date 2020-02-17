/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.aggregations.sketches.FrequentItemsSketch;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.Clip;
import com.yahoo.sketches.frequencies.ErrorType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;

public class TopK extends SketchingStrategy<FrequentItemsSketch> {
    public static final String NEW_NAME_FIELD = "newName";
    public static final String DEFAULT_NEW_NAME = "COUNT";

    public static final String NO_FALSE_NEGATIVES = "NFN";
    public static final String NO_FALSE_POSITIVES = "NFP";

    public static final String THRESHOLD_FIELD = "threshold";

    private final String newName;

    /**
     * Constructor that requires an {@link Aggregation} and a {@link BulletConfig} configuration.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     * @param config The config that has relevant configs for this strategy.
     */
    @SuppressWarnings("unchecked")
    public TopK(Aggregation aggregation, BulletConfig config) {
        super(aggregation, config);

        String errorConfiguration = config.getAs(BulletConfig.TOP_K_AGGREGATION_SKETCH_ERROR_TYPE, String.class);

        ErrorType errorType = getErrorType(errorConfiguration);

        Map<String, Object> attributes = aggregation.getAttributes();

        newName = attributes == null ? DEFAULT_NEW_NAME :
                                       attributes.getOrDefault(NEW_NAME_FIELD, DEFAULT_NEW_NAME).toString();

        int maxMapSize = config.getAs(BulletConfig.TOP_K_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        Number threshold = getThreshold(attributes);
        int size = aggregation.getSize();
        BulletRecordProvider provider = config.getBulletRecordProvider();
        sketch = threshold != null ? new FrequentItemsSketch(errorType, maxMapSize, threshold.longValue(), size, provider) :
                                     new FrequentItemsSketch(errorType, maxMapSize, size, provider);
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return Utilities.isEmpty(fields) ? Optional.of(singletonList(REQUIRES_FIELD_ERROR)) : Optional.empty();
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
        /*
        String field = record.getAndRemove(FrequentItemsSketch.ITEM_FIELD).toString();
        List<String> values = decomposeField(field);
        for (int i = 0; i < fields.size(); ++i) {
            String originalField = fields.get(i);
            String fieldName = fieldsToNames.get(originalField);
            record.setString(Utilities.isEmpty(fieldName) ? originalField : fieldName, values.get(i));
        }
        record.rename(FrequentItemsSketch.COUNT_FIELD, newName);
        */
        String field = record.getAndRemove(FrequentItemsSketch.ITEM_FIELD).toString();
        Long count = (Long) record.getAndRemove(FrequentItemsSketch.COUNT_FIELD);
        List<String> values = decomposeField(field);
        for (int i = 0; i < fields.size(); ++i) {
            String originalField = fields.get(i);
            String fieldName = fieldsToNames.get(originalField);
            record.setString(Utilities.isEmpty(fieldName) ? originalField : fieldName, values.get(i));
        }
        record.setLong(newName, count);
    }

    private static Number getThreshold(Map<String, Object> attributes)  {
        if (Utilities.isEmpty(attributes)) {
            return null;
        }
        return Utilities.getCasted(attributes, THRESHOLD_FIELD, Number.class);
    }

    private static ErrorType getErrorType(String errorType) {
        if (NO_FALSE_POSITIVES.equals(errorType)) {
            return ErrorType.NO_FALSE_POSITIVES;
        }
        return ErrorType.NO_FALSE_NEGATIVES;
    }
}
