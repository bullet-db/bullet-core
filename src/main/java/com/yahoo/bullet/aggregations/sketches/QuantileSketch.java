/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.common.Utilities.round;
import static com.yahoo.bullet.result.Meta.addIfNonNull;

/**
 * Wraps operations for working with a {@link DoublesSketch} - Quantile Sketch.
 */
public class QuantileSketch extends DualSketch {
    private UpdateDoublesSketch updateSketch;
    private DoublesUnion unionSketch;
    private DoublesSketch merged;

    private double[] points;
    private Integer numberOfPoints;
    private int rounding;
    private final Distribution.Type type;

    public static final double QUANTILE_MIN = 0.0;
    public static final double QUANTILE_MAX = 1.0;

    public static final String QUANTILE_FIELD = "Quantile";
    public static final String VALUE_FIELD = "Value";
    public static final String PROBABILITY_FIELD = "Probability";
    public static final String COUNT_FIELD = "Count";
    public static final String RANGE_FIELD = "Range";

    public static final String START_INCLUSIVE = "[";
    public static final String START_EXCLUSIVE = "(";
    public static final String END_EXCLUSIVE = ")";
    public static final String SEPARATOR = " to ";
    public static final String INFINITY = "\u221e";
    public static final String POSITIVE_INFINITY = "+"  + INFINITY;
    public static final String NEGATIVE_INFINITY = "-"  + INFINITY;
    public static final String NEGATIVE_INFINITY_START = START_EXCLUSIVE + NEGATIVE_INFINITY;
    public static final String POSITIVE_INFINITY_END = POSITIVE_INFINITY + END_EXCLUSIVE;

    private QuantileSketch(int k, Distribution.Type type) {
        updateSketch = new DoublesSketchBuilder().build(k);
        unionSketch = new DoublesUnionBuilder().setMaxK(k).build();
        this.type = type;
    }

    /**
     * Creates a quantile sketch with the given number of entries getting results with the given points.
     *
     * @param k A number representative of the size of the sketch.
     * @param type A {@link Distribution.Type} that determines what the points mean.
     * @param points An array of points to get the quantiles, PMF and/or CDF for.
     */
    public QuantileSketch(int k, Distribution.Type type, double[] points) {
        this(k, type);
        this.points = points;
    }

    /**
     * Creates a quantile sketch with the given number of entries generating results with the number of
     * points (evenly-spaced).
     *
     * @param k A number representative of the size of the sketch.
     * @param rounding A number representing how many max decimal places points should have.
     * @param type A {@link Distribution.Type} that determines what the points mean.
     * @param numberOfPoints A positive number of evenly spaced points in the range for the type to get the data for.
     */
    public QuantileSketch(int k, int rounding, Distribution.Type type, int numberOfPoints) {
        this(k, type);
        this.rounding = Math.abs(rounding);
        this.numberOfPoints = numberOfPoints;
    }

    /**
     * Updates the sketch with a double.
     *
     * @param data A double to insert into the sketch.
     */
    public void update(double data) {
        updateSketch.update(data);
        super.update();
    }

    @Override
    public void union(byte[] serialized) {
        DoublesSketch sketch = DoublesSketch.heapify(new NativeMemory(serialized));
        unionSketch.update(sketch);
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
        double[] domain = getDomain();
        double[] range;
        if (type == Distribution.Type.QUANTILE) {
            range = merged.getQuantiles(domain);
        } else if (type == Distribution.Type.PMF) {
            range = merged.getPMF(domain);
        } else {
            range = merged.getCDF(domain);
        }
        return zip(domain, range, type, getNumberOfEntries());
    }

    @Override
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        collect();
        Clip data = super.getResult(metaKey, conceptKeys);
        data.add(getRecords());
        return data;
    }

    @Override
    protected Map<String, Object> addMetadata(Map<String, String> conceptKeys) {
        collect();
        Map<String, Object> metadata = super.addMetadata(conceptKeys);

        addIfNonNull(metadata, conceptKeys, Concept.SKETCH_MINIMUM_VALUE, this::getMinimum);
        addIfNonNull(metadata, conceptKeys, Concept.SKETCH_MAXIMUM_VALUE, this::getMaximum);
        addIfNonNull(metadata, conceptKeys, Concept.SKETCH_ITEMS_SEEN, this::getNumberOfEntries);
        addIfNonNull(metadata, conceptKeys, Concept.SKETCH_NORMALIZED_RANK_ERROR, this::getNormalizedRankError);

        return metadata;
    }

    @Override
    protected void collectUpdateAndUnionSketch() {
        unionSketch.update(updateSketch);
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

    @Override
    protected Boolean isEstimationMode() {
        return merged.isEstimationMode();
    }

    @Override
    protected String getFamily() {
        return Family.QUANTILES.getFamilyName();
    }

    @Override
    protected Integer getSize() {
        return merged.getStorageBytes();
    }

    private Double getMinimum() {
        return merged.getMinValue();
    }

    private Double getMaximum() {
        return merged.getMaxValue();
    }

    private Long getNumberOfEntries() {
        return merged.getN();
    }

    private Double getNormalizedRankError() {
        return merged.getNormalizedRankError();
    }

    private double[] getDomain() {
        if (numberOfPoints != null) {
            return type == Distribution.Type.QUANTILE ? getPoints(QUANTILE_MIN, QUANTILE_MAX, numberOfPoints, rounding) :
                                                        getPoints(getMinimum(), getMaximum(), numberOfPoints, rounding);
        }
        return points;
    }

    /**
     * Exposed for testing only.
     *
     * Creates a {@link List} of {@link BulletRecord} for each corresponding entry in domain and range. The domain
     * is first converted into range names depending on the type.
     *
     * @param domain An array of split points of size N greater than 0.
     * @param range  An array of values for each range in domain: of size N + 1
     *               if type is not {@link Distribution.Type#QUANTILE} else N.
     * @param type The {@link Distribution.Type} to zip for.
     * @param n A long to scale the value of each range entry by if type is not {@link Distribution.Type#QUANTILE}.
     * @return The records that correspond to the data.
     */
    static List<BulletRecord> zip(double[] domain, double[] range, Distribution.Type type, long n) {
        List<BulletRecord> records = null;
        switch (type) {
            case QUANTILE:
                records = zipQuantiles(domain, range);
                break;
            case PMF:
                records = zipRanges(domain, range, n, false);
                break;
            case CDF:
                records = zipRanges(domain, range, n, true);
                break;
        }
        return records;
    }

    // Static helpers

    private static double[] getPoints(double start, double end, int numberOfPoints, int rounding) {
        // We should have numberOfPoints >= 1 but just in case...
        if  (numberOfPoints <= 1 || start >= end) {
            return new double[] { round(start, rounding) };
        }

        double[] points = new double[numberOfPoints];

        // Subtract one to generate [start, start + increment, ..., start + (N-1)*increment]
        int count = numberOfPoints - 1;
        double increment = (end - start) / count;
        double begin = start;
        for (int i = 0; i < count; ++i) {
            points[i] = round(begin, rounding);
            begin += increment;
        }
        // Add start + N*increment = end after
        points[count] = round(end, rounding);
        return points;
    }


    private static List<BulletRecord> zipQuantiles(double[] domain, double[] range) {
        List<BulletRecord> records = new ArrayList<>();

        for (int i = 0; i < domain.length; ++i) {
            records.add(new BulletRecord().setDouble(QUANTILE_FIELD, domain[i])
                                          .setDouble(VALUE_FIELD, range[i]));
        }
        return records;
    }

    private static List<BulletRecord> zipRanges(double[] domain, double[] range, long n, boolean cumulative) {
        List<BulletRecord> records = new ArrayList<>();
        String[] bins = makeBins(domain, cumulative);
        for (int i = 0; i < bins.length; ++i) {
            records.add(new BulletRecord().setString(RANGE_FIELD, bins[i])
                                          .setDouble(PROBABILITY_FIELD, range[i])
                                          .setDouble(COUNT_FIELD, range[i] * n));
        }
        return records;
    }

    private static String[] makeBins(double[] splits, boolean cumulative) {
        String[] bins = new String[splits.length + 1];
        int lastIndex = splits.length - 1;
        return cumulative ? makeCDFBins(bins, splits, lastIndex) : makePMFBins(bins, splits, lastIndex);
    }

    private static String[] makePMFBins(String[] bins, double[] splits, int lastIndex) {
        // The bins are created from (-infinity to splits[0]), [split[1] to split[2]), ..., [split[N] to infinity)
        String prefix = NEGATIVE_INFINITY_START + SEPARATOR;
        for (int i = 0; i <= lastIndex; ++i) {
            double binEnd = splits[i];
            bins[i] = prefix + binEnd + END_EXCLUSIVE;
            prefix = START_INCLUSIVE + binEnd + SEPARATOR;
        }
        bins[lastIndex + 1] = START_INCLUSIVE + splits[lastIndex] + SEPARATOR + POSITIVE_INFINITY_END;
        return bins;
    }

    private static String[] makeCDFBins(String[] bins, double[] splits, int lastIndex) {
        // The bins are created from (-infinity to splits[0]), (-infinity to split[1]), ..., (-infinity to +infinity)
        for (int i = 0; i <= lastIndex; ++i) {
            double binEnd = splits[i];
            bins[i] = NEGATIVE_INFINITY_START + SEPARATOR + binEnd + END_EXCLUSIVE;
        }
        bins[lastIndex + 1] = NEGATIVE_INFINITY_START + SEPARATOR + POSITIVE_INFINITY_END;
        return bins;
    }
}
