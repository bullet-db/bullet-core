/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.aggregations.sketches.ThetaSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class CountDistinct extends KMVStrategy<ThetaSketch> {
    public static final String NEW_NAME_FIELD = "newName";
    public static final String DEFAULT_NEW_NAME = "COUNT DISTINCT";

    // Theta Sketch defaults
    // Recommended for real-time systems
    public static final String DEFAULT_UPDATE_SKETCH_FAMILY = Family.ALPHA.getFamilyName();
    // This gives us (Alpha sketches fall back to QuickSelect RSEs after compaction or set operations) a 2.34% error
    // rate at 99.73% confidence (3 Standard Deviations).
    public static final int DEFAULT_NOMINAL_ENTRIES = 16384;

    private final String newName;

    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public CountDistinct(Aggregation aggregation) {
        super(aggregation);
        Map<String, Object> attributes = aggregation.getAttributes();

        ResizeFactor resizeFactor = getResizeFactor(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR);
        float samplingProbability = config.getAs(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING, Float.class);
        Family family = getFamily(config.getAs(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY, String.class));
        int nominalEntries = config.getAs(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        newName = attributes == null ? DEFAULT_NEW_NAME :
                                       attributes.getOrDefault(NEW_NAME_FIELD, DEFAULT_NEW_NAME).toString();

        sketch = new ThetaSketch(resizeFactor, family, samplingProbability, nominalEntries);
    }

    @Override
    public List<Error> initialize() {
        return Utilities.isEmpty(fields) ? singletonList(REQUIRES_FIELD_ERROR) : null;
    }

    @Override
    public void consume(BulletRecord data) {
        String field = composeField(data);
        sketch.update(field);
    }

    @Override
    public Clip getAggregation() {
        Clip result = super.getAggregation();
        // One record only
        BulletRecord record = result.getRecords().get(0);
        record.rename(ThetaSketch.COUNT_FIELD, newName);
        return result;
    }

    /**
     * Convert a String family into a {@link Family}. This is used to recognize a user input family choice.
     *
     * @param family The string version of the {@link Family}. Currently, QuickSelect and Alpha are supported.
     * @return The Sketch family represented by the string or {@link #DEFAULT_UPDATE_SKETCH_FAMILY} otherwise.
     */
    static Family getFamily(String family) {
        return Family.QUICKSELECT.getFamilyName().equals(family) ? Family.QUICKSELECT : Family.ALPHA;
    }
}
