/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
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
    private Sketch result;

    private String family;

    public static final String COUNT_FIELD = "count";

    /**
     * Constructor for creating a theta sketch.
     *
     * @param resizeFactor The {@link ResizeFactor} to use for the sketch.
     * @param family The {@link Family} to use.
     * @param samplingProbability The sampling probability to use.
     * @param nominalEntries The nominal entries for the sketch.
     * @param provider A BulletRecordProvider to generate BulletRecords.
     */
    public ThetaSketch(ResizeFactor resizeFactor, Family family, float samplingProbability, int nominalEntries,
                       BulletRecordProvider provider) {
        updateSketch = UpdateSketch.builder().setFamily(family).setNominalEntries(nominalEntries)
                                             .setP(samplingProbability).setResizeFactor(resizeFactor)
                                             .build();
        unionSketch = SetOperation.builder().setNominalEntries(nominalEntries).setP(samplingProbability)
                                            .setResizeFactor(resizeFactor).buildUnion();
        this.family = family.getFamilyName();
        this.provider = provider;
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
    public byte[] serialize() {
        merge();
        return result.toByteArray();
    }

    @Override
    public List<BulletRecord> getRecords() {
        merge();
        List<BulletRecord> result = new ArrayList<>();
        result.add(getCount());
        return result;
    }

    @Override
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        merge();
        Clip data = super.getResult(metaKey, conceptKeys);
        return data.add(getCount());
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
        unionSketch.update(updateSketch.compact(false, null));
        updateSketch.reset();
        mergeUnionSketch();
    }

    @Override
    protected void mergeUpdateSketch() {
        result = updateSketch.compact(false, null);
        updateSketch.reset();
    }

    @Override
    protected void mergeUnionSketch() {
        result = unionSketch.getResult(false, null);
        unionSketch.reset();
    }

    @Override
    protected boolean unionedExistingResults() {
        unionSketch.update(result);
        return result != null;
    }

    // Metadata

    @Override
    protected Boolean isEstimationMode() {
        return result.isEstimationMode();
    }

    @Override
    protected String getFamily() {
        return family;
    }

    @Override
    protected Integer getSize() {
        return result.getCurrentBytes(true);
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

    private BulletRecord getCount() {
        double count = result.getEstimate();
        BulletRecord record = provider.getInstance();
        record.setLong(COUNT_FIELD, Math.round(count));
        //record.setDouble(COUNT_FIELD, count);
        return record;
    }
}
