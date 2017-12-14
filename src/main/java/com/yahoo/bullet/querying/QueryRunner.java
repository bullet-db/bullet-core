/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.operations.FilterOperations;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.parsing.FilterClause;
import com.yahoo.bullet.parsing.Parser;
import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.common.Utilities.extractField;
import static com.yahoo.bullet.common.Utilities.splitField;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.REGEX_LIKE;
import static com.yahoo.bullet.operations.FilterOperations.RELATIONAL_OPERATORS;

@Slf4j
public class QueryRunner {
    private Query query;
    @Getter @Setter
    private long startTime;
    private long lastResultTime = 0L;

    private Boolean shouldInjectTimestamp;
    private String timestampKey;

    public static final String AGGREGATION_FAILURE_RESOLUTION = "Please try again later";

    /**
     * Constructor that takes a String representation of the query and a configuration to use.
     *
     * @param queryString The query as a string.
     * @param config The validated {@link BulletConfig} configuration to use.
     * @throws ParsingException if there was an issue.
     */
    public QueryRunner(String queryString, BulletConfig config) throws JsonParseException, ParsingException {
        query = Parser.parse(queryString, config);
        shouldInjectTimestamp = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP, Boolean.class);
        timestampKey = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP_KEY, String.class);

        Optional<List<Error>> errors = query.initialize();
        if (errors.isPresent()) {
            throw new ParsingException(errors.get());
        }
    }

    public boolean consume(BulletRecord record) {
        // If query is expired, not accepting data or does not match filters, don't consume...
        if (isExpired() || !isAcceptingData() || !filter(record)) {
            return false;
        }
        aggregate(project(record));
        return isMicroBatch();
    }

    public boolean combine(byte[] data) {
        aggregate(data);
        // If the query is no longer accepting data, then the Query has been satisfied.
        return !isAcceptingData();
    }

    public boolean combine(QueryRunner query) {
        return combine(query.getData());
    }

    public byte[] getData() {
        return getSerializedAggregate();
    }

    public Clip getResult() {
        Clip result = getAggregate();
        lastResultTime = System.currentTimeMillis();
        return result;
    }

    /**
     * Returns true iff the query has expired.
     *
     * @return boolean denoting if query has expired.
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > startTime + query.getDuration();
    }

    @Override
    public String toString() {
        return query.toString();
    }

    /**
     * Runs the specification on this record and returns true if this record matched its filters.
     *
     * @param record The input record.
     * @return true if this record matches this specification's filters.
     */
    private boolean filter(BulletRecord record) {
        List<Clause> filters = query.getFilters();
        // Add the record if we have no filters
        if (filters == null) {
            return true;
        }
        // Otherwise short circuit evaluate till the first filter fails. Filters are ANDed.
        return filters.stream().allMatch(c -> check(c, record));
    }

    /**
     * Checks to see if this expression is satisfied.
     *
     * @param record The record to check the expression for.
     * @return true iff this expression is satisfied.
     */
    private boolean check(FilterClause clause, BulletRecord record) {
        FilterOperations.FilterType operation = clause.getOperation();
        List values = clause.getValues();
        if (operation == null || values == null || values.isEmpty()) {
            return true;
        }
        return compare(operation, extractField(field, record), values);
    }

    @SuppressWarnings("unchecked")
    private boolean compare(FilterOperations.FilterType operation, Object value, List<String> values) {
        TypedObject typed = new TypedObject(value);
        return operation == REGEX_LIKE ? RELATIONAL_OPERATORS.get(REGEX_LIKE).compare(typed, getValuesAsPatterns(values)) :
                RELATIONAL_OPERATORS.get(operation).compare(typed, values);
    }

    /**
     * Run the specification's projections on the record.
     *
     * @param record The input record.
     * @return The projected record.
     */
    private BulletRecord project(BulletRecord record) {
        Projection projection = query.getProjection();
        BulletRecord projected = projection != null ? project(projection.getFields(), record) : record;
        return addAdditionalFields(projected);
    }

    /**
     * Applies the projection.
     * @param record The record to project from.
     * @return The projected record.
     */
    private BulletRecord project(Map<String, String> fields, BulletRecord record) {
        // Returning the record itself if no projections. The record itself should never be modified so it's ok.
        if (fields == null) {
            return record;
        }
        // More efficient if fields << the fields in the BulletRecord
        BulletRecord projected = new BulletRecord();
        for (Map.Entry<String, String> e : fields.entrySet()) {
            String field = e.getKey();
            String newName = e.getValue();
            try {
                copyInto(projected, newName, record, field);
            } catch (ClassCastException cce) {
                log.warn("Skipping copying {} as {} as it is not a field that can be extracted", field, newName);
            }
        }
        return projected;
    }

    private void copyInto(BulletRecord record, String newName, BulletRecord source, String field) throws ClassCastException {
        if (field == null) {
            return;
        }
        String[] split = splitField(field);
        if (split.length > 1) {
            record.set(newName, source, split[0], split[1]);
        } else {
            record.set(newName, source, field);
        }
    }

    /**
     * Presents the aggregation with a {@link BulletRecord}.
     *
     * @param record The record to insert into the aggregation.
     */
    private void aggregate(BulletRecord record) {
        try {
            query.getAggregation().getStrategy().consume(record);
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
    private void aggregate(byte[] data) {
        try {
            query.getAggregation().getStrategy().combine(data);
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
    private byte[] getSerializedAggregate() {
        try {
            return query.getAggregation().getStrategy().getSerializedAggregation();
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
    private Clip getAggregate() {
        try {
            return query.getAggregation().getStrategy().getAggregation();
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
    private boolean isAcceptingData() {
        return query.getAggregation().getStrategy().isAcceptingData();
    }

    /**
     * Checks to see if this specification has reached a micro-batch size.
     *
     * @return a boolean denoting whether the specification has reached a micro-batch size.
     */
    private boolean isMicroBatch() {
        return true;
    }

    private BulletRecord addAdditionalFields(BulletRecord record) {
        if (shouldInjectTimestamp) {
            record.setLong(timestampKey, System.currentTimeMillis());
        }
        return record;
    }
}

