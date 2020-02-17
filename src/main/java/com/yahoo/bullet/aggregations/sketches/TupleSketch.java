/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.aggregations.grouping.CachingGroupData;
import com.yahoo.bullet.aggregations.grouping.GroupData;
import com.yahoo.bullet.aggregations.grouping.GroupDataSummary;
import com.yahoo.bullet.aggregations.grouping.GroupDataSummaryFactory;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta.Concept;
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

import static com.yahoo.bullet.result.Meta.addIfNonNull;

public class TupleSketch extends KMVSketch {
    private UpdatableSketch<CachingGroupData, GroupDataSummary> updateSketch;
    private Union<GroupDataSummary> unionSketch;
    private Sketch<GroupDataSummary> result;

    private final int maxSize;
    /**
     * Initialize a tuple sketch for summarizing group data.
     *
     * @param resizeFactor The {@link ResizeFactor} to use for the sketch.
     * @param samplingProbability The sampling probability to use.
     * @param nominalEntries The nominal entries for the sketch.
     * @param maxSize The maximum size of groups to return.
     * @param provider A BulletRecordProvider to generate BulletRecords.
     */
    @SuppressWarnings("unchecked")
    public TupleSketch(ResizeFactor resizeFactor, float samplingProbability, int nominalEntries, int maxSize, BulletRecordProvider provider) {
        GroupDataSummaryFactory factory = new GroupDataSummaryFactory();
        UpdatableSketchBuilder<CachingGroupData, GroupDataSummary> builder = new UpdatableSketchBuilder(factory);

        updateSketch = builder.setResizeFactor(resizeFactor)
                              .setNominalEntries(nominalEntries)
                              .setSamplingProbability(samplingProbability)
                              .build();
        unionSketch = new Union<>(nominalEntries, factory);

        this.maxSize = maxSize;
        this.provider = provider;
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
    public byte[] serialize() {
        merge();
        return result.toByteArray();
    }

    @Override
    public List<BulletRecord> getRecords() {
        merge();
        List<BulletRecord> result = new ArrayList<>();
        SketchIterator<GroupDataSummary> iterator = this.result.iterator();
        for (int count = 0; iterator.next() && count < maxSize; count++) {
            GroupData data = iterator.getSummary().getData();
            result.add(data.getAsBulletRecord(provider));
        }
        return result;
    }

    @Override
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        merge();
        Clip result = super.getResult(metaKey, conceptKeys);
        result.add(getRecords());
        return result;
    }

    @Override
    public void reset() {
        result = null;
        updateSketch.reset();
        unionSketch.reset();
        super.reset();
    }

    @Override
    protected void mergeBothSketches() {
        unionSketch.update(updateSketch.compact());
        updateSketch.reset();
        mergeUnionSketch();
    }

    @Override
    protected void mergeUpdateSketch() {
        result = updateSketch.compact();
        updateSketch.reset();
    }

    @Override
    protected void mergeUnionSketch() {
        result = unionSketch.getResult();
        unionSketch.reset();
    }

    @Override
    protected boolean unionedExistingResults() {
        unionSketch.update(result);
        return result != null;
    }

    @Override
    protected Map<String, Object> addMetadata(Map<String, String> conceptKeys) {
        // The super will call merge()
        Map<String, Object> metadata = super.addMetadata(conceptKeys);
        addIfNonNull(metadata, conceptKeys, Concept.SKETCH_UNIQUES_ESTIMATE, this::getUniquesEstimate);
        return metadata;
    }

    // Meta

    @Override
    protected Boolean isEstimationMode() {
        return result.isEstimationMode();
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
        return result.getTheta();
    }

    @Override
    protected Double getLowerBound(int standardDeviation) {
        return result.getLowerBound(standardDeviation);
    }

    @Override
    protected Double getUpperBound(int standardDeviation) {
        return result.getUpperBound(standardDeviation);
    }

    /**
     * Returns the estimate of the uniques in the Sketch. Only applicable after {@link #merge()}.
     *
     * @return A Double representing the number of unique values in the Sketch.
     */
    private Double getUniquesEstimate() {
        return result.getEstimate();
    }
}
