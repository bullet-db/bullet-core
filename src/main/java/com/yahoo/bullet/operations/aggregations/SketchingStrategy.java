/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.aggregations.sketches.Sketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Specification;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The parent class for all {@link Strategy} that use Sketches.
 *
 * @param <S> A {@link Sketch} type.
 */
public abstract class SketchingStrategy<S extends Sketch> implements Strategy {
    // The metadata concept to key mapping
    protected final Map<String, String> metadataKeys;
    // A  copy of the configuration
    protected final Map config;

    // Separator for multiple fields when inserting into the Sketch
    protected final String separator;

    // The fields being inserted into the Sketch
    protected final Map<String, String> fieldsToNames;
    protected final List<String> fields;

    // The Sketch that should be initialized by a child class
    protected S sketch;

    /**
     * The constructor for creating a Sketch based strategy.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public SketchingStrategy(Aggregation aggregation) {
        config = aggregation.getConfiguration();
        metadataKeys = (Map<String, String>) config.getOrDefault(BulletConfig.RESULT_METADATA_METRICS_MAPPING,
                                                                 Collections.emptyMap());
        separator = config.getOrDefault(BulletConfig.AGGREGATION_COMPOSITE_FIELD_SEPARATOR,
                                        Aggregation.DEFAULT_FIELD_SEPARATOR).toString();

        fieldsToNames = aggregation.getFields();
        fields = Utilities.isEmpty(fieldsToNames) ? Collections.emptyList() : new ArrayList<>(fieldsToNames.keySet());

    }

    @Override
    public void combine(byte[] serializedAggregation) {
        sketch.union(serializedAggregation);
    }

    @Override
    public byte[] getSerializedAggregation() {
        return sketch.serialize();
    }

    @Override
    public Clip getAggregation() {
        String metakey = metadataKeys.getOrDefault(Metadata.Concept.SKETCH_METADATA.getName(), null);
        return sketch.getResult(metakey, metadataKeys);
    }

    /**
     * Extracts the fields in a pre-determined order from {@link BulletRecord} as one String with the separator.
     *
     * @param record The non-null record containing data for the fields.
     * @return A string representing the composite field.
     */
    public String composeField(BulletRecord record) {
        return composeField(fields.stream().map(field -> Objects.toString(Specification.extractField(field, record))));
    }

    /**
     * Composes a {@link Stream} of strings together into one string with the separator.
     *
     * @param fields The fields to combine.
     * @return A string that represents the fields.
     */
    public String composeField(Stream<String> fields) {
        return fields.collect(Collectors.joining(separator));
    }

    /**
     * Breaks down a composite field into individual fields.
     *
     * @param field The composite field to break down.
     * @return A {@link List} of the fields that this field was made of.
     */
    public List<String> decomposeField(String field) {
        return Arrays.asList(field.split(Pattern.quote(separator)));
    }
}
