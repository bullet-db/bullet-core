/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.typesystem.Type;
import com.yahoo.bullet.operations.typesystem.TypedObject;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is the top level Bullet Query Specification. It holds the definition of the Query.
 */
@Getter @Setter(AccessLevel.PACKAGE) @Slf4j
public class Specification implements Configurable, Validatable  {
    @Expose
    private Projection projection;
    @Expose
    private List<Clause> filters;
    @Expose
    private Aggregation aggregation;
    @Expose
    private Integer duration;

    private Boolean shouldInjectTimestamp;
    private String timestampKey;

    public static final String DEFAULT_RECEIVE_TIMESTAMP_KEY = "bullet_receive_timestamp";
    public static final Integer DEFAULT_DURATION_MS = 30 * 1000;
    public static final Integer DEFAULT_MAX_DURATION_MS = 120 * 1000;
    public static final String SUB_KEY_SEPERATOR = "\\.";

    public static final String AGGREGATION_FAILURE_RESOLUTION = "Please try again later";

    /**
     * Default constructor. GSON recommended.
     */
    public Specification() {
        filters = null;
        // If no aggregation is provided, the default one is used. Aggregations must be present.
        aggregation = new Aggregation();
    }

    /**
     * Runs the specification on this record and returns true if this record matched its filters.
     *
     * @param record The input record.
     * @return true if this record matches this specification's filters.
    */
    public boolean filter(BulletRecord record) {
        // Add the record if we have no filters
        if (filters == null) {
            return true;
        }
        // Otherwise short circuit evaluate till the first filter fails. Filters are ANDed.
        return filters.stream().allMatch(c -> c.check(record));
    }

    /**
     * Run the specification's projections on the record.
     *
     * @param record The input record.
     * @return The projected record.
     */
    public BulletRecord project(BulletRecord record) {
        BulletRecord projected = projection != null ? projection.project(record) : record;
        return addAdditionalFields(projected);
    }

    /**
     * Presents the aggregation with a {@link BulletRecord}.
     *
     * @param record The record to insert into the aggregation.
     */
    public void aggregate(BulletRecord record) {
        try {
            aggregation.getStrategy().consume(record);
        } catch (RuntimeException e) {
            log.error("Unable to consume {} for query {}", record, this);
            log.error("Skipping due to", e);
        }
    }

    /**
     * Presents the aggregation with a serialized data representation of a prior aggregation.
     *
     * @param data The serialized data that represents a partial aggregation.
     */
    public void aggregate(byte[] data) {
        try {
            aggregation.getStrategy().combine(data);
        } catch (RuntimeException e) {
            log.error("Unable to aggregate {} for query {}", data, this);
            log.error("Skipping due to", e);
        }
    }

    /**
     * Get the aggregate matched records so far.
     *
     * @return The byte[] representation of the serialized aggregate.
     */
    public byte[] getSerializedAggregate() {
        try {
            return aggregation.getStrategy().getSerializedAggregation();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized aggregation for query {}", this);
            log.error("Skipping due to", e);
            return null;
        }
    }

    /**
     * Gets the aggregated records {@link Clip}.
     *
     * @return a non-null {@link Clip} representing the aggregation.
     */
    public Clip getAggregate() {
        try {
            return aggregation.getStrategy().getAggregation();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized aggregation for query {}", this);
            log.error("Skipping due to", e);
            return Clip.of(Metadata.of(Error.makeError(e.getMessage(), AGGREGATION_FAILURE_RESOLUTION)));
        }
    }

    /**
     * Checks to see if this specification is accepting more data.
     *
     * @return a boolean denoting whether more data should be presented to this specification.
     */
    public boolean isAcceptingData() {
        return aggregation.getStrategy().isAcceptingData();
    }

    /**
     * Checks to see if this specification has reached a micro-batch size.
     *
     * @return a boolean denoting whether the specification has reached a micro-batch size.
     */
    public boolean isMicroBatch() {
        return aggregation.getStrategy().isMicroBatch();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map configuration) {
        if (filters != null) {
            filters.forEach(f -> f.configure(configuration));
        }
        if (projection != null) {
            projection.configure(configuration);
        }
        // Must have an aggregation
        if (aggregation == null) {
            aggregation = new Aggregation();
        }
        aggregation.configure(configuration);

        shouldInjectTimestamp = (Boolean) configuration.getOrDefault(BulletConfig.RECORD_INJECT_TIMESTAMP, false);
        timestampKey = (String) configuration.getOrDefault(BulletConfig.RECORD_INJECT_TIMESTAMP_KEY, DEFAULT_RECEIVE_TIMESTAMP_KEY);

        Number defaultDuration = (Number) configuration.getOrDefault(BulletConfig.SPECIFICATION_DEFAULT_DURATION, DEFAULT_DURATION_MS);
        Number maxDuration = (Number) configuration.getOrDefault(BulletConfig.SPECIFICATION_MAX_DURATION, DEFAULT_MAX_DURATION_MS);
        int durationDefault = defaultDuration.intValue();
        int durationMax = maxDuration.intValue();

        // Null or negative, then default, else min of duration and max.
        duration = (duration == null || duration < 0) ? durationDefault : Math.min(duration, durationMax);
    }

    private BulletRecord addAdditionalFields(BulletRecord record) {
        if (shouldInjectTimestamp) {
            record.setLong(timestampKey, System.currentTimeMillis());
        }
        return record;
    }

    @Override
    public Optional<List<Error>> validate() {
        List<Error> errors = new ArrayList<>();
        if (filters != null) {
            for (Clause clause : filters) {
                clause.validate().ifPresent(errors::addAll);
            }
        }
        if (projection != null) {
            projection.validate().ifPresent(errors::addAll);
        }
        if (aggregation != null) {
            aggregation.validate().ifPresent(errors::addAll);
        }
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    /**
     * Extracts the field from the given {@link BulletRecord}.
     *
     * @param field The field to get. It can be "." separated to look inside maps.
     * @param record The record containing the field.
     * @return The extracted field or null if error or not found.
     */
    public static Object extractField(String field, BulletRecord record) {
        if (field == null) {
            return null;
        }
        String[] split = getFields(field);
        try {
            return split.length > 1 ? record.get(split[0], split[1]) : record.get(field);
        } catch (ClassCastException cce) {
            return null;
        }
    }

    /**
     * Extracts the field from the given (@link BulletRecord} as a {@link Number}, if possible.
     *
     * @param field The field containing a numeric value to get. It can be "." separated to look inside maps.
     * @param record The record containing the field.
     * @return The value of the field as a {@link Number} or null if it cannot be forced to one.
     */
    public static Number extractFieldAsNumber(String field, BulletRecord record) {
        Object value = extractField(field, record);
        // Also checks for null
        if (value instanceof Number) {
            return (Number) value;
        }
        TypedObject asNumber = TypedObject.makeNumber(value);
        if (asNumber.getType() == Type.UNKNOWN) {
            return null;
        }
        return (Number) asNumber.getValue();
    }

    /**
     * Takes a field and returns it split into subfields if necessary.
     *
     * @param field The non-null field to get.
     * @return The field split into field or subfield if it was a map field, or just the field itself.
     */
    public static String[] getFields(String field) {
        return field.split(SUB_KEY_SEPERATOR, 2);
    }

    @Override
    public String toString() {
        return "{filters: " + filters + ", projection: " + projection + ", aggregation: " + aggregation +
                ", duration: " + duration + "}";
    }
}
