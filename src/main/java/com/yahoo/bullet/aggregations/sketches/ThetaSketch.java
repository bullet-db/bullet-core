/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ThetaSketch extends KMVSketch {
    private UpdateSketch updateSketch;
    private Union unionSketch;
    private Sketch merged;

    private String family;

    public static final String COUNT_FIELD = "count";

    /**
     * Constructor for creating a theta sketch.
     *
     * @param resizeFactor The {@link ResizeFactor} to use for the sketch.
     * @param family The {@link Family} to use.
     * @param samplingProbability The sampling probability to use.
     * @param nominalEntries The nominal entries for the sketch.
     */
    public ThetaSketch(ResizeFactor resizeFactor, Family family, float samplingProbability, int nominalEntries) {
        updateSketch = UpdateSketch.builder().setFamily(family).setNominalEntries(nominalEntries)
                                             .setP(samplingProbability).setResizeFactor(resizeFactor)
                                             .build();
        unionSketch = SetOperation.builder().setNominalEntries(nominalEntries).setP(samplingProbability)
                                            .setResizeFactor(resizeFactor).buildUnion();
        this.family = family.getFamilyName();
    }

    /**
     * Update the sketch with a String field.
     *
     * @param field The field to present to the sketch.
     */
    public void update(String field) {
        updateSketch.update(field);
        super.update();
    }

    @Override
    public void union(byte[] serialized) {
        Sketch deserialized = Sketches.wrapSketch(new NativeMemory(serialized));
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
    public List<BulletRecord> getRecords() {
        collect();
        List<BulletRecord> result = new ArrayList<>();
        result.add(getCount());
        return result;
    }

    @Override
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        collect();
        Clip data = super.getResult(metaKey, conceptKeys);
        return data.add(getCount());
    }

    @Override
    protected void collectUpdateAndUnionSketch() {
        unionSketch.update(updateSketch.compact(false, null));
        collectUnionSketch();
    }

    @Override
    protected void collectUpdateSketch() {
        merged = updateSketch.compact(false, null);
    }

    @Override
    protected void collectUnionSketch() {
        merged = unionSketch.getResult(false, null);
    }

    // Metadata

    @Override
    protected Boolean isEstimationMode() {
        return merged.isEstimationMode();
    }

    @Override
    protected String getFamily() {
        return family;
    }

    @Override
    protected Integer getSize() {
        return merged.getCurrentBytes(true);
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

    private BulletRecord getCount() {
        double count = merged.getEstimate();
        BulletRecord record = new BulletRecord();
        record.setDouble(COUNT_FIELD, count);
        return record;

    }
}
