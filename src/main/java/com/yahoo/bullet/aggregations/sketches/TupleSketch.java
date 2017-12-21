/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.aggregations.grouping.GroupData;
import com.yahoo.bullet.aggregations.grouping.GroupDataSummary;
import com.yahoo.bullet.aggregations.grouping.GroupDataSummaryFactory;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;
import com.yahoo.sketches.tuple.Union;
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TupleSketch extends KMVSketch {
    private UpdatableSketch<CachingGroupData, GroupDataSummary> updateSketch;
    private Union<GroupDataSummary> unionSketch;
    private Sketch<GroupDataSummary> merged;

    private final int maxSize;

    /**
     * Initialize a tuple sketch for summarizing group data.
     *
     * @param resizeFactor The {@link ResizeFactor} to use for the sketch.
     * @param samplingProbability The sampling probability to use.
     * @param nominalEntries The nominal entries for the sketch.
     * @param maxSize The maximum size of groups to return.
     */
    @SuppressWarnings("unchecked")
    public TupleSketch(ResizeFactor resizeFactor, float samplingProbability, int nominalEntries, int maxSize) {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        UpdatableSketchBuilder<CachingGroupData, GroupDataSummary> builder = new UpdatableSketchBuilder(factory);

        updateSketch = builder.setResizeFactor(resizeFactor).setNominalEntries(nominalEntries)
                              .setSamplingProbability(samplingProbability).build();
        unionSketch = new Union<>(nominalEntries, factory);

        this.maxSize = maxSize;
    }

    /**
     * Update the sketch with a key representing a group and the data for it.
     *
     * @param key The key to present the data to the sketch as.
     * @param data The data for the group.
     */
    public void update(String key, CachingGroupData data) {
        updateSketch.update(key, data);
        super.update();
    }

    @Override
    public void union(byte[] serialized) {
        Sketch<GroupDataSummary> deserialized = Sketches.heapifySketch(new NativeMemory(serialized));
        unionSketch.update(deserialized);
        super.union();
    }

    @Override
    public void reset() {
        unionSketch.reset();
        updateSketch.reset();
        super.reset();
    }

    @Override
    public byte[] serialize() {
        collect();
        return merged.toByteArray();
    }

    @Override
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        collect();
        Clip result = super.getResult(metaKey, conceptKeys);
        result.add(getRecords());
        return result;
    }

    @Override
    public List<BulletRecord> getRecords() {
        collect();
        List<BulletRecord> result = new ArrayList<>();
        SketchIterator<GroupDataSummary> iterator = merged.iterator();
        for (int count = 0; iterator.next() && count < maxSize; count++) {
            GroupData data = iterator.getSummary().getData();
            result.add(data.getAsBulletRecord());
        }
        return result;
    }

    @Override
    protected Map<String, Object> addMetadata(Map<String, String> conceptKeys) {
        // The super will call collect()
        Map<String, Object> metadata = super.addMetadata(conceptKeys);
        addIfNonNull(metadata, conceptKeys.get(Concept.UNIQUES_ESTIMATE.getName()), this::getUniquesEstimate);
        return metadata;
    }

    @Override
    protected void collectUpdateAndUnionSketch() {
        unionSketch.update(updateSketch.compact());
        collectUnionSketch();
    }

    @Override
    protected void collectUpdateSketch() {
        merged = updateSketch.compact();
    }

    @Override
    protected void collectUnionSketch() {
        merged = unionSketch.getResult();
    }

    // Metadata

    @Override
    protected Boolean isEstimationMode() {
        return merged.isEstimationMode();
    }

    @Override
    protected String getFamily() {
        return Family.TUPLE.getFamilyName();
    }

    @Override
    protected Integer getSize() {
        // Size need not be calculated since Summaries are arbitrarily large
        return null;
    }

    @Override
    protected Double getTheta() {
        return merged.getTheta();
    }

    @Override
    protected Double getLowerBound(int standardDeviation) {
        return merged.getLowerBound(standardDeviation);
    }

    @Override
    protected Double getUpperBound(int standardDeviation) {
        return merged.getUpperBound(standardDeviation);
    }

    /**
     * Returns the estimate of the uniques in the Sketch. Only applicable after {@link #collect()}.
     *
     * @return A Double representing the number of unique values in the Sketch.
     */
    private Double getUniquesEstimate() {
        return merged.getEstimate();

    }
}
