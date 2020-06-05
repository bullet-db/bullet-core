/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.pubsub.PubSub.Context;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.typesystem.Schema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class BulletConfig extends Config {
    // Field names
    public static final String QUERY_DEFAULT_DURATION = "bullet.query.default.duration.ms";
    public static final String QUERY_MAX_DURATION = "bullet.query.max.duration.ms";

    public static final String AGGREGATION_DEFAULT_SIZE = "bullet.query.aggregation.default.size";
    public static final String AGGREGATION_MAX_SIZE = "bullet.query.aggregation.max.size";
    public static final String AGGREGATION_COMPOSITE_FIELD_SEPARATOR = "bullet.query.aggregation.composite.field.separator";

    public static final String RAW_AGGREGATION_MAX_SIZE = "bullet.query.aggregation.raw.max.size";

    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES = "bullet.query.aggregation.count.distinct.sketch.entries";
    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING = "bullet.query.aggregation.count.distinct.sketch.sampling";
    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY = "bullet.query.aggregation.count.distinct.sketch.family";
    public static final String COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR = "bullet.query.aggregation.count.distinct.sketch.resize.factor";

    public static final String GROUP_AGGREGATION_SKETCH_ENTRIES = "bullet.query.aggregation.group.sketch.entries";
    public static final String GROUP_AGGREGATION_MAX_SIZE = "bullet.query.aggregation.group.max.size";
    public static final String GROUP_AGGREGATION_SKETCH_SAMPLING = "bullet.query.aggregation.group.sketch.sampling";
    public static final String GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR = "bullet.query.aggregation.group.sketch.resize.factor";

    public static final String DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES = "bullet.query.aggregation.distribution.sketch.entries";
    public static final String DISTRIBUTION_AGGREGATION_MAX_POINTS = "bullet.query.aggregation.distribution.max.points";
    public static final String DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING = "bullet.query.aggregation.distribution.generated.points.rounding";

    public static final String TOP_K_AGGREGATION_SKETCH_ENTRIES = "bullet.query.aggregation.top.k.sketch.entries";
    public static final String TOP_K_AGGREGATION_SKETCH_ERROR_TYPE = "bullet.query.aggregation.top.k.sketch.error.type";

    public static final String RESULT_METADATA_ENABLE = "bullet.result.metadata.enable";
    public static final String RESULT_METADATA_METRICS = "bullet.result.metadata.metrics";
    public static final String RESULT_METADATA_METRICS_CONCEPT_KEY = "name";
    public static final String RESULT_METADATA_METRICS_NAME_KEY = "key";

    public static final String WINDOW_DISABLE = "bullet.query.window.disable";
    public static final String WINDOW_MIN_EMIT_EVERY = "bullet.query.window.min.emit.every.ms";

    public static final String RATE_LIMIT_ENABLE = "bullet.query.rate.limit.enable";
    public static final String RATE_LIMIT_MAX_EMIT_COUNT = "bullet.query.rate.limit.max.emit.count";
    public static final String RATE_LIMIT_TIME_INTERVAL = "bullet.query.rate.limit.time.interval";

    public static final String PUBSUB_CONTEXT_NAME = "bullet.pubsub.context.name";
    public static final String PUBSUB_CLASS_NAME = "bullet.pubsub.class.name";

    public static final String STORAGE_CLASS_NAME = "bullet.storage.class.name";

    public static final String METRIC_PUBLISHER_CLASS_NAME = "bullet.metric.publisher.class.name";

    public static final String RECORD_PROVIDER_CLASS_NAME = "bullet.record.provider.class.name";
    public static final String RECORD_SCHEMA_FILE_NAME = "bullet.record.schema.file.name";

    public static final String QUERY_PARTITIONER_ENABLE = "bullet.query.partitioner.enable";
    public static final String QUERY_PARTITIONER_CLASS_NAME = "bullet.query.partitioner.class.name";
    public static final String EQUALITY_PARTITIONER_FIELDS = "bullet.query.partitioner.equality.fields";
    public static final String EQUALITY_PARTITIONER_DELIMITER = "bullet.query.partitioner.equality.delimiter";

    // Defaults
    public static final long DEFAULT_QUERY_DURATION = (long) Double.POSITIVE_INFINITY;
    public static final long DEFAULT_QUERY_MAX_DURATION = (long) Double.POSITIVE_INFINITY;

    public static final int DEFAULT_AGGREGATION_SIZE = 500;
    public static final int DEFAULT_AGGREGATION_MAX_SIZE = 500;
    public static final String DEFAULT_AGGREGATION_COMPOSITE_FIELD_SEPARATOR = "|";

    public static final int DEFAULT_RAW_AGGREGATION_MAX_SIZE = 100;

    public static final String QUICKSELECT_SKETCH_FAMILY = "QuickSelect";
    public static final String ALPHA_SKETCH_FAMILY = "Alpha";
    public static final int DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES = 16384;
    public static final float DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING = 1.0f;
    public static final String DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY = ALPHA_SKETCH_FAMILY;
    public static final int DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR = 8;

    public static final int DEFAULT_GROUP_AGGREGATION_SKETCH_ENTRIES = 512;
    public static final int DEFAULT_GROUP_AGGREGATION_MAX_SIZE = 500;
    public static final float DEFAULT_GROUP_AGGREGATION_SKETCH_SAMPLING = 1.0f;
    public static final int DEFAULT_GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR = 8;

    public static final int DEFAULT_DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES = 1024;
    public static final int DEFAULT_DISTRIBUTION_AGGREGATION_MAX_POINTS = 100;
    public static final int DEFAULT_DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING = 6;

    public static final int DEFAULT_TOP_K_AGGREGATION_SKETCH_ENTRIES = 1024;
    public static final String DEFAULT_TOP_K_AGGREGATION_SKETCH_ERROR_TYPE = "NFN";

    public static final boolean DEFAULT_RESULT_METADATA_ENABLE = true;
    /** The mapping from Metadata concepts to the keys with which they should logged in the result metadata. */
    public static final List<Map<String, String>> DEFAULT_RESULT_METADATA_METRICS =
        makeMetadata(ImmutablePair.of(Concept.QUERY_METADATA, "Query"),
                     ImmutablePair.of(Concept.QUERY_ID, "ID"),
                     ImmutablePair.of(Concept.QUERY_BODY, "Body"),
                     ImmutablePair.of(Concept.QUERY_RECEIVE_TIME, "Receive Time"),
                     ImmutablePair.of(Concept.QUERY_FINISH_TIME, "Finish Time"),
                     ImmutablePair.of(Concept.SKETCH_METADATA, "Sketch"),
                     ImmutablePair.of(Concept.SKETCH_ESTIMATED_RESULT, "Was Estimated"),
                     ImmutablePair.of(Concept.SKETCH_STANDARD_DEVIATIONS, "Standard Deviations"),
                     ImmutablePair.of(Concept.SKETCH_FAMILY, "Family"),
                     ImmutablePair.of(Concept.SKETCH_SIZE, "Size"),
                     ImmutablePair.of(Concept.SKETCH_THETA, "Theta"),
                     ImmutablePair.of(Concept.SKETCH_UNIQUES_ESTIMATE, "Uniques Estimate"),
                     ImmutablePair.of(Concept.SKETCH_MINIMUM_VALUE, "Minimum Value"),
                     ImmutablePair.of(Concept.SKETCH_MAXIMUM_VALUE, "Maximum Value"),
                     ImmutablePair.of(Concept.SKETCH_ITEMS_SEEN, "Items Seen"),
                     ImmutablePair.of(Concept.SKETCH_NORMALIZED_RANK_ERROR, "Normalized Rank Error"),
                     ImmutablePair.of(Concept.SKETCH_MAXIMUM_COUNT_ERROR, "Maximum Count Error"),
                     ImmutablePair.of(Concept.SKETCH_ACTIVE_ITEMS, "Active Items"),
                     ImmutablePair.of(Concept.WINDOW_METADATA, "Window"),
                     ImmutablePair.of(Concept.WINDOW_NAME, "Name"),
                     ImmutablePair.of(Concept.WINDOW_NUMBER, "Number"),
                     ImmutablePair.of(Concept.WINDOW_SIZE, "Size"),
                     ImmutablePair.of(Concept.WINDOW_EMIT_TIME, "Emit Time"),
                     ImmutablePair.of(Concept.WINDOW_EXPECTED_EMIT_TIME, "Expected Emit Time"));

    public static final boolean DEFAULT_WINDOW_DISABLE = false;
    public static final int DEFAULT_WINDOW_MIN_EMIT_EVERY = 1000;

    public static final boolean DEFAULT_RATE_LIMIT_ENABLE = true;
    public static final long DEFAULT_RATE_LIMIT_MAX_EMIT_COUNT = 50;
    public static final long DEFAULT_RATE_LIMIT_TIME_INTERVAL = 100;

    public static final String DEFAULT_PUBSUB_CONTEXT_NAME = Context.QUERY_PROCESSING.name();
    public static final String DEFAULT_PUBSUB_CLASS_NAME = "com.yahoo.bullet.pubsub.MockPubSub";

    public static final String DEFAULT_RECORD_PROVIDER_CLASS_NAME = "com.yahoo.bullet.record.AvroBulletRecordProvider";

    public static final boolean DEFAULT_QUERY_PARTITIONER_ENABLE = false;
    public static final String DEFAULT_QUERY_PARTITIONER_CLASS_NAME = "com.yahoo.bullet.querying.partitioning.SimpleEqualityPartitioner";
    public static final String DEFAULT_EQUALITY_PARTITIONER_DELIMITER = "|";
    public static final int MAXIMUM_EQUALITY_FIELDS = 10;

    // Validator definitions for the configs in this class.
    // This can be static since VALIDATOR itself does not change for different values for fields in the BulletConfig.
    private static final Validator VALIDATOR = new Validator();
    private static final long serialVersionUID = 8074017371059016233L;
    public static final String DEFAULT_CONFIGURATION_NAME = "bullet_defaults.yaml";

    static {
        VALIDATOR.define(QUERY_DEFAULT_DURATION)
                 .defaultTo(DEFAULT_QUERY_DURATION)
                 .checkIf(Validator::isPositive)
                 .castTo(Validator::asLong);
        VALIDATOR.define(QUERY_MAX_DURATION)
                 .defaultTo(DEFAULT_QUERY_MAX_DURATION)
                 .checkIf(Validator::isPositive)
                 .castTo(Validator::asLong);

        VALIDATOR.define(AGGREGATION_DEFAULT_SIZE)
                 .defaultTo(DEFAULT_AGGREGATION_SIZE)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);
        VALIDATOR.define(AGGREGATION_MAX_SIZE)
                 .defaultTo(DEFAULT_AGGREGATION_MAX_SIZE)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);
        VALIDATOR.define(AGGREGATION_COMPOSITE_FIELD_SEPARATOR)
                 .defaultTo(DEFAULT_AGGREGATION_COMPOSITE_FIELD_SEPARATOR)
                 .checkIf(Validator::isString);

        VALIDATOR.define(RAW_AGGREGATION_MAX_SIZE)
                 .defaultTo(DEFAULT_RAW_AGGREGATION_MAX_SIZE)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);

        VALIDATOR.define(COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES)
                 .defaultTo(DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_ENTRIES)
                 .checkIf(Validator::isPowerOfTwo)
                 .castTo(Validator::asInt);
        VALIDATOR.define(COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING)
                 .defaultTo(DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_SAMPLING)
                 .checkIf(Validator::isFloat)
                 .checkIf(Validator.isInRange(0.0, 1.0))
                 .castTo(Validator::asFloat);
        VALIDATOR.define(COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY)
                 .defaultTo(DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_FAMILY)
                 .checkIf(Validator::isString)
                 .checkIf(Validator.isIn(ALPHA_SKETCH_FAMILY, QUICKSELECT_SKETCH_FAMILY));
        VALIDATOR.define(COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR)
                 .defaultTo(DEFAULT_COUNT_DISTINCT_AGGREGATION_SKETCH_RESIZE_FACTOR)
                 .checkIf(Validator::isPowerOfTwo)
                 .checkIf(Validator.isInRange(1, 8))
                 .castTo(Validator::asInt);

        VALIDATOR.define(GROUP_AGGREGATION_SKETCH_ENTRIES)
                 .defaultTo(DEFAULT_GROUP_AGGREGATION_SKETCH_ENTRIES)
                 .checkIf(Validator::isPowerOfTwo)
                 .castTo(Validator::asInt);
        VALIDATOR.define(GROUP_AGGREGATION_MAX_SIZE)
                 .defaultTo(DEFAULT_GROUP_AGGREGATION_MAX_SIZE)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);
        VALIDATOR.define(GROUP_AGGREGATION_SKETCH_SAMPLING)
                 .defaultTo(DEFAULT_GROUP_AGGREGATION_SKETCH_SAMPLING)
                 .checkIf(Validator::isFloat)
                 .checkIf(Validator.isInRange(0.0, 1.0))
                 .castTo(Validator::asFloat);
        VALIDATOR.define(GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR)
                 .defaultTo(DEFAULT_GROUP_AGGREGATION_SKETCH_RESIZE_FACTOR)
                 .checkIf(Validator::isPowerOfTwo)
                 .checkIf(Validator.isInRange(1, 8))
                 .castTo(Validator::asInt);

        VALIDATOR.define(DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES)
                 .defaultTo(DEFAULT_DISTRIBUTION_AGGREGATION_SKETCH_ENTRIES)
                 .checkIf(Validator::isPowerOfTwo)
                 .castTo(Validator::asInt);
        VALIDATOR.define(DISTRIBUTION_AGGREGATION_MAX_POINTS)
                 .defaultTo(DEFAULT_DISTRIBUTION_AGGREGATION_MAX_POINTS)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);
        VALIDATOR.define(DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING)
                 .defaultTo(DEFAULT_DISTRIBUTION_AGGREGATION_GENERATED_POINTS_ROUNDING)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);

        VALIDATOR.define(TOP_K_AGGREGATION_SKETCH_ENTRIES)
                 .defaultTo(DEFAULT_TOP_K_AGGREGATION_SKETCH_ENTRIES)
                 .checkIf(Validator::isPowerOfTwo)
                 .castTo(Validator::asInt);
        VALIDATOR.define(TOP_K_AGGREGATION_SKETCH_ERROR_TYPE)
                 .defaultTo(DEFAULT_TOP_K_AGGREGATION_SKETCH_ERROR_TYPE)
                 .checkIf(Validator::isString);

        VALIDATOR.define(RESULT_METADATA_ENABLE)
                 .defaultTo(DEFAULT_RESULT_METADATA_ENABLE)
                 .checkIf(Validator::isBoolean);
        VALIDATOR.define(RESULT_METADATA_METRICS)
                 .defaultTo(DEFAULT_RESULT_METADATA_METRICS)
                 .checkIf(Validator::isList)
                 .checkIf(BulletConfig::isMetadata)
                 .castTo(BulletConfig::mapifyMetadata)
                 .unless(BulletConfig::alreadySetMetadata);

        VALIDATOR.define(WINDOW_DISABLE)
                 .defaultTo(DEFAULT_WINDOW_DISABLE)
                 .checkIf(Validator::isBoolean);
        VALIDATOR.define(WINDOW_MIN_EMIT_EVERY)
                 .defaultTo(DEFAULT_WINDOW_MIN_EMIT_EVERY)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);

        VALIDATOR.define(RATE_LIMIT_ENABLE)
                 .defaultTo(DEFAULT_RATE_LIMIT_ENABLE)
                 .checkIf(Validator::isBoolean);
        VALIDATOR.define(RATE_LIMIT_MAX_EMIT_COUNT)
                 .defaultTo(DEFAULT_RATE_LIMIT_MAX_EMIT_COUNT)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);
        VALIDATOR.define(RATE_LIMIT_TIME_INTERVAL)
                 .defaultTo(DEFAULT_RATE_LIMIT_TIME_INTERVAL)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);

        VALIDATOR.define(PUBSUB_CONTEXT_NAME)
                 .defaultTo(DEFAULT_PUBSUB_CONTEXT_NAME)
                 .checkIf(Validator.isIn(Context.QUERY_PROCESSING.name(), Context.QUERY_SUBMISSION.name()));
        VALIDATOR.define(PUBSUB_CLASS_NAME)
                 .defaultTo(DEFAULT_PUBSUB_CLASS_NAME)
                 .checkIf(Validator::isClassName);

        VALIDATOR.define(RECORD_PROVIDER_CLASS_NAME)
                 .defaultTo(DEFAULT_RECORD_PROVIDER_CLASS_NAME)
                 .checkIf(Validator::isClassName);
        VALIDATOR.define(RECORD_SCHEMA_FILE_NAME)
                 .checkIf(Validator::isString)
                 .unless(Validator::isNull);

        VALIDATOR.define(QUERY_PARTITIONER_ENABLE)
                 .defaultTo(DEFAULT_QUERY_PARTITIONER_ENABLE)
                 .checkIf(Validator::isBoolean);
        VALIDATOR.define(QUERY_PARTITIONER_CLASS_NAME)
                 .defaultTo(DEFAULT_QUERY_PARTITIONER_CLASS_NAME)
                 .checkIf(Validator::isClassName);
        VALIDATOR.define(EQUALITY_PARTITIONER_FIELDS)
                 .checkIf(Validator.isListOfType(String.class))
                 .checkIf(Validator.hasMaximumListSize(MAXIMUM_EQUALITY_FIELDS))
                 .unless(Validator::isNull)
                 .orFail();
        VALIDATOR.define(EQUALITY_PARTITIONER_DELIMITER)
                 .defaultTo(DEFAULT_EQUALITY_PARTITIONER_DELIMITER)
                 .checkIf(Validator::isString);


        VALIDATOR.relate("Max should be >= default", QUERY_MAX_DURATION, QUERY_DEFAULT_DURATION)
                 .checkIf(Validator::isGreaterOrEqual);
        VALIDATOR.relate("Max should be >= default", AGGREGATION_MAX_SIZE, AGGREGATION_DEFAULT_SIZE)
                 .checkIf(Validator::isGreaterOrEqual);
        VALIDATOR.relate("Raw max should be <= Aggregation max", AGGREGATION_MAX_SIZE, RAW_AGGREGATION_MAX_SIZE)
                 .checkIf(Validator::isGreaterOrEqual);
        VALIDATOR.relate("Group max should be <= Aggregation max", AGGREGATION_MAX_SIZE, GROUP_AGGREGATION_MAX_SIZE)
                 .checkIf(Validator::isGreaterOrEqual);
        VALIDATOR.relate("Distribution points should be <= Aggregation max", AGGREGATION_MAX_SIZE, DISTRIBUTION_AGGREGATION_MAX_POINTS)
                 .checkIf(Validator::isGreaterOrEqual);
        VALIDATOR.relate("Max duration should be >= min window emit interval", QUERY_MAX_DURATION, WINDOW_MIN_EMIT_EVERY)
                 .checkIf(Validator::isGreaterOrEqual);
        VALIDATOR.relate("If metadata is enabled, keys should be defined", RESULT_METADATA_ENABLE, RESULT_METADATA_METRICS)
                 .checkIf(BulletConfig::isMetadataConfigured);
        VALIDATOR.relate("If metadata is disabled, keys should not be defined", RESULT_METADATA_ENABLE, RESULT_METADATA_METRICS)
                 .checkIf(BulletConfig::isMetadataNecessary)
                 .orElseUse(false, Collections.emptyMap());

        VALIDATOR.evaluate("If the equality partitioner is used, the partitioner fields should be defined",
                            QUERY_PARTITIONER_ENABLE, QUERY_PARTITIONER_CLASS_NAME, EQUALITY_PARTITIONER_FIELDS)
                 .checkIf(BulletConfig::areEqualityPartitionerFieldsDefined)
                 .orFail();
    }

    // Members
    private BulletRecordProvider provider;

    /**
     * Constructor that loads specific file augmented with defaults and validates itself.
     *
     * @param file YAML file to load.
     */
    public BulletConfig(String file) {
        super(file, DEFAULT_CONFIGURATION_NAME);
        VALIDATOR.validate(this);
        provider = BulletRecordProvider.from(getAs(RECORD_PROVIDER_CLASS_NAME, String.class));
    }

    /**
     * Constructor that loads just the defaults.
     */
    public BulletConfig() {
        super(DEFAULT_CONFIGURATION_NAME);
        VALIDATOR.validate(this);
        provider = BulletRecordProvider.from(getAs(RECORD_PROVIDER_CLASS_NAME, String.class));
    }

    /**
     * Get the {@link BulletRecordProvider} stored in this BulletConfig instance. This BulletRecordProvider is
     * created when this BulletConfig is constructed.
     *
     * @return The BulletRecordProvider instance.
     */
    public BulletRecordProvider getBulletRecordProvider() {
        return provider;
    }

    public Schema getSchema() {
        String schemaFile = getAs(RECORD_SCHEMA_FILE_NAME, String.class);
        if (schemaFile == null) {
            return null;
        }
        return new Schema(schemaFile);
    }

    /**
     * Validates and fixes configuration for this config. If there are undefaulted or wrongly typed elements, you
     * should use a {@link Validator} to define the appropriate definitions, casters and defaults to use. You
     * should call this method before you use the config if you set additional settings to ensure that all configurations
     * are valid.
     *
     * This class defines a validator for all the fields it knows about. If you subclass it and define your own fields,
     * you should {@link #getValidator()} and add entries and relationships that you need to validate. Make sure
     * to call this method from your override if you wish validate your new definitions.
     *
     * @return This config for chaining.
     */
    public BulletConfig validate() {
        VALIDATOR.validate(this);
        return this;
    }

    /**
     * Returns a copy of the {@link Validator} used by this config. You can use this to extend it with more
     * {@link Validator.Entry} and {@link Validator.Relationship} items.
     *
     * @return A copy of the Validator object used.
     */
    public static Validator getValidator() {
        return VALIDATOR.copy();
    }

    @Override
    public void merge(Config other) {
        super.merge(other);
        validate();
    }

    /**
     * This method loads a given class name (stored in this config) with the class name key and creates an instance of
     * it by using a constructor that has a single argument for a {@link BulletConfig}. It then passes in this config
     * and returns the constructed instance.
     *
     * @param classNameKey The name of the key which stores the class name to load in this config.
     * @param <S> The type of the class.
     * @return A created instance of this class.
     * @throws RuntimeException if there were issues creating an instance. It wraps the real exception.
     */
    public <S> S loadConfiguredClass(String classNameKey) {
        return Utilities.loadConfiguredClass(this.getAs(classNameKey, String.class), this);
    }

    @SuppressWarnings("unchecked")
    private static Object mapifyMetadata(Object metadata) {
        List<Map> entries = (List<Map>) metadata;
        Map<String, String> mapping = new HashMap<>();
        // For each metadata entry that is configured, load the name of the field to add it to the metadata as.
        for (Map m : entries) {
            mapping.put((String) m.get(RESULT_METADATA_METRICS_CONCEPT_KEY),
                        (String) m.get(RESULT_METADATA_METRICS_NAME_KEY));
        }
        return mapping;
    }

    @SuppressWarnings("unchecked")
    private static boolean alreadySetMetadata(Object metadata) {
        if (metadata == null || !(metadata instanceof Map)) {
            return false;
        }
        try {
            Map<String, String> casted = (Map<String, String>) metadata;
            return casted.keySet().stream().allMatch(c -> Meta.KNOWN_CONCEPTS.contains(Concept.from(c)));
        } catch (ClassCastException e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean isMetadata(Object metadata) {
        try {
            for (Map m : (List<Map>) metadata) {
                if (m.size() != 2) {
                    log.warn("Meta should only contain the keys {}, {}. Found {}", RESULT_METADATA_METRICS_CONCEPT_KEY,
                             RESULT_METADATA_METRICS_NAME_KEY, m);
                    return false;
                }
                String concept = (String) m.get(RESULT_METADATA_METRICS_CONCEPT_KEY);
                String name = (String) m.get(RESULT_METADATA_METRICS_CONCEPT_KEY);
                if (!Meta.KNOWN_CONCEPTS.contains(Concept.from(concept))) {
                    log.warn("Unknown metadata concept: {}", concept);
                    return false;
                }
            }
        } catch (ClassCastException e) {
            log.warn("Meta should be a list containing maps of string keys and values. Found {}", metadata);
            return false;
        }
        return true;
    }

    private static boolean isMetadataConfigured(Object enabled, Object keys) {
        // This function should return false when metadata is enabled but keys are not set.
        boolean isMetadataOff = !((Boolean) enabled);
        return isMetadataOff || keys != null;
    }

    private static boolean isMetadataNecessary(Object enabled, Object keys) {
        // This function should return false when metadata is disabled but keys are set.
        boolean isMetadataOn = (Boolean) enabled;
        return isMetadataOn || keys == null;
    }

    @SafeVarargs
    private static List<Map<String, String>> makeMetadata(Pair<Concept, String>... entries) {
        List<Map<String, String>> metadataList = new ArrayList<>();
        for (Pair<Concept, String> entry : entries) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put(RESULT_METADATA_METRICS_CONCEPT_KEY, entry.getKey().getName());
            metadata.put(RESULT_METADATA_METRICS_NAME_KEY, entry.getValue());
            metadataList.add(metadata);
        }
        return metadataList;
    }

    @SuppressWarnings("unchecked")
    private static boolean areEqualityPartitionerFieldsDefined(List<Object> fields) {
        boolean enabled = (Boolean) fields.get(0);
        if (!enabled) {
            return true;
        }
        String className = fields.get(1).toString();
        if (!DEFAULT_QUERY_PARTITIONER_CLASS_NAME.equals(className)) {
            return true;
        }
        List<String> partitionFields = ((List<String>) fields.get(2));
        return  partitionFields != null && !partitionFields.isEmpty();
    }
}
