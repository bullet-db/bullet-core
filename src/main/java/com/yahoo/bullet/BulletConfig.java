/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class BulletConfig extends Config {
    public static final String SPECIFICATION_DEFAULT_DURATION = "bullet.query.default.duration";
    public static final String SPECIFICATION_MAX_DURATION = "bullet.query.max.duration";
    public static final String AGGREGATION_DEFAULT_SIZE = "bullet.query.aggregation.default.size";
    public static final String AGGREGATION_MAX_SIZE = "bullet.query.aggregation.max.size";
    public static final String AGGREGATION_COMPOSITE_FIELD_SEPARATOR = "bullet.query.aggregation.composite.field.separator";

    public static final String RAW_AGGREGATION_MAX_SIZE = "bullet.query.aggregation.raw.max.size";
    public static final String RAW_AGGREGATION_MICRO_BATCH_SIZE = "bullet.query.aggregation.raw.micro.batch.size";

    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES = "bullet.query.aggregation.count.distinct.sketch.entries";
    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING = "bullet.query.aggregation.count.distinct.sketch.sampling";
    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY = "bullet.query.aggregation.count.distinct.sketch.family";
    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR = "bullet.query.aggregation.count.distinct.sketch.resize.factor";

    public static final String GROUP_AGGREGATION_SKETCH_ENTRIES = "bullet.query.aggregation.group.sketch.entries";
    public static final String GROUP_AGGREGATION_SKETCH_SAMPLING = "bullet.query.aggregation.group.sketch.sampling";
    public static final String GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR = "bullet.query.aggregation.group.sketch.resize.factor";

    public static final String DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES = "bullet.query.aggregation.distribution.sketch.entries";
    public static final String DISTRIBUTION_AGGREGATION_MAX_POINTS = "bullet.query.aggregation.distribution.max.points";
    public static final String DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING = "bullet.query.aggregation.distribution.generated.points.rounding";

    public static final String TOP_K_AGGREGATION_SKETCH_ENTRIES = "bullet.query.aggregation.top.k.sketch.entries";
    public static final String TOP_K_AGGREGATION_SKETCH_ERROR_TYPE = "bullet.query.aggregation.top.k.sketch.error.type";

    public static final String RECORD_INJECT_TIMESTAMP = "bullet.record.inject.timestamp.enable";
    public static final String RECORD_INJECT_TIMESTAMP_KEY = "bullet.record.inject.timestamp.key";

    public static final String RESULT_METADATA_ENABLE = "bullet.result.metadata.enable";
    public static final String RESULT_METADATA_METRICS = "bullet.result.metadata.metrics";
    public static final String RESULT_METADATA_METRICS_CONCEPT_KEY = "name";
    public static final String RESULT_METADATA_METRICS_NAME_KEY = "key";

    public static final String RESULT_METADATA_METRICS_MAPPING = "bullet.result.metadata.metrics.mapping";

    public static final String PUBSUB_CONTEXT_NAME = "bullet.pubsub.context.name";
    public static final String PUBSUB_CLASS_NAME = "bullet.pubsub.class.name";

    public static final String DEFAULT_CONFIGURATION_NAME = "bullet_defaults.yaml";

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     * @throws IOException if an error occurred with the file loading.
     */
    public BulletConfig(String file) throws IOException {
        super(file, DEFAULT_CONFIGURATION_NAME);
    }
}
