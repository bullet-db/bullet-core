/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.parsing.Parser;
import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

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

    /**
     * Consume a {@link BulletRecord} for this query. The record may or not be actually incorporated into the query
     * results. This depends on whether the query can accept more data, if it is expired or not or if the record matches
     * any query filtering criteria.
     *
     * @param record The BulletRecord to consume.
     * @return A boolean denoting whether the query has reached a batch size.
     */
    public boolean consume(BulletRecord record) {
        // If query is expired, not accepting data or does not match the filters, don't consume...
        if (isExpired() || !isAcceptingData() || !filter(record)) {
            return false;
        }
        aggregate(project(record));
        return isMicroBatch();
    }

    /**
     * Presents the query with a serialized data representation of a prior result for the query.
     *
     * @param data The serialized data that represents a partial query result.
     */
    public boolean merge(byte[] data) {
        try {
            query.getAggregation().getStrategy().combine(data);
        } catch (RuntimeException e) {
            log.error("Unable to aggregate {} for query {}", data, this);
            log.error("Skipping due to", e);
        }
        // If the query is no longer accepting data, then the Query has been satisfied.
        return !isAcceptingData();
    }

    public boolean merge(QueryRunner query) {
        return merge(query.getData());
    }

    /**
     * Get the result emitted so far after the last window.
     *
     * @return The byte[] representation of the serialized result.
     */
    public byte[] getData() {
        try {
            return query.getAggregation().getStrategy().getSerializedAggregation();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized aggregation for query {}", this);
            log.error("Skipping due to", e);
            return null;
        }
    }

    /**
     * Gets the resulting {@link Clip} of the results so far.
     *
     * @return a non-null {@link Clip} representing the aggregated result.
     */
    public Clip getResult() {
        Clip result;
        try {
            result = query.getAggregation().getStrategy().getAggregation();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized aggregation for query {}", this);
            log.error("Skipping due to", e);
            result = Clip.of(Metadata.of(Error.makeError(e.getMessage(), AGGREGATION_FAILURE_RESOLUTION)));
        }
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
        return filters.stream().allMatch(c -> FilterOperations.perform(record, c));
    }

    /**
     * Run the specification's projections on the record.
     *
     * @param record The input record.
     * @return The projected record.
     */
    private BulletRecord project(BulletRecord record) {
        Projection projection = query.getProjection();
        BulletRecord projected = projection != null ? ProjectionOperations.project(record, projection) : record;
        return addAdditionalFields(projected);
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

    @Override
    public String toString() {
        return query.toString();
    }
}

