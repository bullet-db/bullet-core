/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.aggregations.sketches.ThetaSketch;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.CountDistinctAggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;

import java.util.List;

public class CountDistinct extends KMVStrategy<ThetaSketch> {
    // Theta Sketch defaults
    // Recommended for real-time systems
    public static final String DEFAULT_UPDATE_SKETCH_FAMILY = Family.ALPHA.getFamilyName();
    // This gives us (Alpha sketches fall back to QuickSelect RSEs after compaction or set operations) a 2.34% error
    // rate at 99.73% confidence (3 Standard Deviations).
    public static final int DEFAULT_NOMINAL_ENTRIES = 16384;

    private final String name;

    /**
     * Constructor that requires an {@link Aggregation} and a {@link BulletConfig} configuration.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     * @param config The config that has relevant configs for this strategy.
     */
    @SuppressWarnings("unchecked")
    public CountDistinct(CountDistinctAggregation aggregation, BulletConfig config) {
        super(aggregation, config);

        ResizeFactor resizeFactor = getResizeFactor(config, BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR);
        float samplingProbability = config.getAs(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING, Float.class);
        Family family = getFamily(config.getAs(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY, String.class));
        int nominalEntries = config.getAs(BulletConfig.COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES, Integer.class);

        name = aggregation.getName();
        sketch = new ThetaSketch(resizeFactor, family, samplingProbability, nominalEntries, config.getBulletRecordProvider());
    }

    @Override
    public void consume(BulletRecord data) {
        String field = composeField(data);
        sketch.update(field);
    }

    @Override
    public Clip getResult() {
        Clip result = super.getResult();
        renameInPlace(result.getRecords());
        return result;
    }

    @Override
    public List<BulletRecord> getRecords() {
        return renameInPlace(super.getRecords());
    }

    private List<BulletRecord> renameInPlace(List<BulletRecord> records) {
        // One record only.
        BulletRecord record = records.get(0);
        record.rename(ThetaSketch.COUNT_FIELD, name);
        return records;
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
