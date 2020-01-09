/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.common.Utilities.extractFieldAsNumber;
import static com.yahoo.bullet.common.BulletError.makeError;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * This {@link Strategy} uses {@link QuantileSketch} to find distributions of a numeric field. Based on the size
 * configured for the sketch, the normalized rank error can be determined and tightly bound.
 */
public class Distribution extends SketchingStrategy<QuantileSketch> {
    @Getter
    public enum Type {
        QUANTILE("QUANTILE"),
        PMF("PMF"),
        CDF("CDF");

        private String name;

        Type(String name) {
            this.name = name;
        }

        /**
         * Checks to see if this String represents this enum.
         *
         * @param name The String version of the enum.
         * @return true if the name represents this enum.
         */
        public boolean isMe(String name) {
            return this.name.equals(name);
        }
    }

    // Distribution fields
    public static final String TYPE = "type";
    public static final String POINTS = "points";
    public static final String RANGE_START = "start";
    public static final String RANGE_END = "end";
    public static final String RANGE_INCREMENT = "increment";
    public static final String NUMBER_OF_POINTS = "numberOfPoints";

    private final int entries;
    private final int maxPoints;
    private final int rounding;

    private String field;

    // Copy of the aggregation
    private Aggregation aggregation;
    private BulletRecordProvider provider;

    // Validation
    public static final Map<String, Type> SUPPORTED_DISTRIBUTION_TYPES = new HashMap<>();
    static {
        SUPPORTED_DISTRIBUTION_TYPES.put(Type.QUANTILE.getName(), Type.QUANTILE);
        SUPPORTED_DISTRIBUTION_TYPES.put(Type.PMF.getName(), Type.PMF);
        SUPPORTED_DISTRIBUTION_TYPES.put(Type.CDF.getName(), Type.CDF);
    }
    public static final BulletError REQUIRES_TYPE_ERROR =
            makeError("The DISTRIBUTION type requires specifying a type", "Please set type to one of: " +
                      String.join(", ", SUPPORTED_DISTRIBUTION_TYPES.keySet()));
    public static final BulletError REQUIRES_POINTS_ERROR =
            makeError("The DISTRIBUTION type requires at least one point specified in attributes",
                      "Please add a list of numeric points with points, OR " +
                      "specify a number of equidistant points to generate with numberOfPoints OR " +
                      "specify a range to generate points for with start, end and increment (start < end, increment > 0)");
    public static final BulletError REQUIRES_POINTS_PROPER_RANGE =
            makeError(Type.QUANTILE.getName() + " requires points in the proper range",
                      "Please add or generate points: 0 <= point <= 1");
    public static final BulletError REQUIRES_ONE_FIELD_ERROR =
            makeError("The aggregation type requires exactly one field", "Please add exactly one field to fields");

    /**
     * Constructor that requires an {@link Aggregation} and a {@link BulletConfig} configuration.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     * @param config The config that has relevant configs for this strategy.
     */
    @SuppressWarnings("unchecked")
    public Distribution(Aggregation aggregation, BulletConfig config) {
        super(aggregation, config);
        entries = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES, Integer.class);
        rounding = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING, Integer.class);

        int pointLimit = config.getAs(BulletConfig.DISTRIBUTION_AGGREGATION_MAX_POINTS, Integer.class);
        maxPoints = Math.min(pointLimit, aggregation.getSize());
        this.aggregation = aggregation;
        this.provider = config.getBulletRecordProvider();

        // The sketch is initialized in initialize!
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (Utilities.isEmpty(fields) || fields.size() != 1) {
            return Optional.of(singletonList(REQUIRES_ONE_FIELD_ERROR));
        }

        Map<String, Object> attributes = aggregation.getAttributes();
        if (Utilities.isEmpty(attributes)) {
            return Optional.of(singletonList(REQUIRES_TYPE_ERROR));
        }

        String typeString = Utilities.getCasted(attributes, TYPE, String.class);
        Type type = SUPPORTED_DISTRIBUTION_TYPES.get(typeString);
        if (type == null) {
            return Optional.of(singletonList(REQUIRES_TYPE_ERROR));
        }

        // Try to initialize sketch now
        sketch = getSketch(entries, maxPoints, rounding, type, attributes, provider);

        if (sketch == null) {
            return Optional.of(type == Type.QUANTILE ?
                               asList(REQUIRES_POINTS_ERROR, REQUIRES_POINTS_PROPER_RANGE) :
                               singletonList(REQUIRES_POINTS_ERROR));
        }

        // Initialize field since we have exactly 1
        field = fields.get(0);

        return Optional.empty();
    }

    @Override
    public void consume(BulletRecord data) {
        Number value = extractFieldAsNumber(field, data);
        if (value != null) {
            sketch.update(value.doubleValue());
        }
    }

    private static QuantileSketch getSketch(int entries, int maxPoints, int rounding, Type type,
                                            Map<String, Object> attributes, BulletRecordProvider provider) {
        int equidistantPoints = getNumberOfEquidistantPoints(attributes);
        if (equidistantPoints > 0) {
            return new QuantileSketch(entries, rounding, type, Math.min(equidistantPoints, maxPoints), provider);
        }
        List<Double> points = getProvidedPoints(attributes);
        if (Utilities.isEmpty(points)) {
            points = generatePoints(maxPoints, rounding, attributes);
        }

        // If still not good, return null
        if (Utilities.isEmpty(points)) {
            return null;
        }
        // Sort and get first maxPoints distinct values
        double[] cleanedPoints = points.stream().distinct().sorted().limit(maxPoints)
                                       .mapToDouble(d -> d).toArray();

        if (invalidBounds(type, cleanedPoints)) {
            return null;
        }

        return new QuantileSketch(entries, type, cleanedPoints, provider);
    }

    private static boolean invalidBounds(Type type, double[] points) {
        // No points or if type is QUANTILE, invalid range if the start < 0 or end > 1
        return points.length < 1 || (type == Type.QUANTILE && (points[0] < 0.0 || points[points.length - 1] > 1.0));
    }

    // Point generation methods

    @SuppressWarnings("unchecked")
    private static List<Double> getProvidedPoints(Map<String, Object> attributes) {
        List<Double> points = Utilities.getCasted(attributes, POINTS, List.class);
        if (!Utilities.isEmpty(points)) {
            return points;
        }
        return Collections.emptyList();
    }

    private static List<Double> generatePoints(int maxPoints, int rounding, Map<String, Object> attributes) {
        Number start = Utilities.getCasted(attributes, RANGE_START, Number.class);
        Number end = Utilities.getCasted(attributes, RANGE_END, Number.class);
        Number increment = Utilities.getCasted(attributes, RANGE_INCREMENT, Number.class);

        if (!areNumbersValid(start, end, increment)) {
            return Collections.emptyList();
        }
        Double from = start.doubleValue();
        Double to = end.doubleValue();
        Double by = increment.doubleValue();
        List<Double> points = new ArrayList<>();
        for (int i = 0; i < maxPoints && from <= to; ++i) {
            points.add(Utilities.round(from, rounding));
            from += by;
        }
        return points;
    }

    private static int getNumberOfEquidistantPoints(Map<String, Object> attributes) {
        Number equidistantPoints = Utilities.getCasted(attributes, NUMBER_OF_POINTS, Number.class);
        if (equidistantPoints == null || equidistantPoints.intValue() < 0) {
            return 0;
        }
        return equidistantPoints.intValue();
    }

    private static boolean areNumbersValid(Number start, Number end, Number increment) {
        if (start == null || end == null || increment == null) {
            return false;
        }
        return start.doubleValue() < end.doubleValue() && increment.doubleValue() > 0;
    }
}
