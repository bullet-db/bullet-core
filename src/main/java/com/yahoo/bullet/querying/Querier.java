/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Queryable;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.Parser;
import com.yahoo.bullet.parsing.ParsingError;
import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;
import com.yahoo.bullet.windowing.Scheme;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.yahoo.bullet.common.Closable.areAnyClosed;

/**
 * This manages a {@link Query} that is currently being executed. It can {@link #consume(BulletRecord)} records for the
 * query, and {@link #combine(byte[])} serialized data from another instance of the running query. It can also merge
 * itself with another instance of running query using {@link #merge(Queryable)}.
 */
@Slf4j
public class Querier implements Serializable, Queryable {
    public static final String AGGREGATION_FAILURE_RESOLUTION = "Please try again later";

    private String id;
    @Setter(AccessLevel.PACKAGE)
    private Query query;

    @Getter @Setter
    private long startTime;

    private Boolean shouldInjectTimestamp;
    private String timestampKey;

    // TODO: Consider serializing the window in some fashion to save compute on calling start.
    @Setter(AccessLevel.PACKAGE)
    private transient Scheme window;

    private BulletConfig config;
    private Map<String, String> metaKeys;

    /**
     * Constructor that takes a configured {@link Query} instance and a configuration to use. This also starts
     * executing the query.
     *
     * @param id The query ID.
     * @param query The query object.
     * @param config The validated {@link BulletConfig} configuration to use.
     * @throws BulletException if there was an issue with setting up the query.
     */
    public Querier(String id, Query query, BulletConfig config) throws BulletException {
        this.id = id;
        this.query = query;
        this.config = config;
        start();
    }

    /**
     * Constructor that takes a String representation of the query and a configuration to use. This also starts the
     * query.
     *
     * @param id The query ID.
     * @param queryString The query as a string.
     * @param config The validated {@link BulletConfig} configuration to use.
     * @throws BulletException if there was an issue.
     * @throws JsonParseException if there was an issue parsing the query.
     */
    public Querier(String id, String queryString, BulletConfig config) throws JsonParseException, BulletException {
        this(id, Parser.parse(queryString, config), config);
    }

    /**
     * Starts the query and throws a {@link BulletException} with any errors if it was not possible to start.
     * You must call this method if you have deserialized an instance of this object from data.
     *
     * @return This object for chaining.
     * @throws BulletException if there were issues starting the query.
     */
    public Querier start() throws BulletException {
        Optional<List<BulletError>> errors = initialize();
        if (errors.isPresent()) {
            throw new BulletException(errors.get());
        }
        return this;
    }

    // ****************** Queryable overrides ******************

    /**
     * Starts the query. You must call this method if you have deserialized an instance of this object from data.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Optional<List<BulletError>> initialize() {
        shouldInjectTimestamp = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP, Boolean.class);
        timestampKey = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP_KEY, String.class);
        metaKeys = (Map<String, String>) config.getAs(BulletConfig.RESULT_METADATA_METRICS, Map.class);

        List<BulletError> errors = new ArrayList<>();

        query.initialize().ifPresent(errors::addAll);

        // Aggregation is guaranteed to not be null and guaranteed to have a proper type.
        Strategy strategy = AggregationOperations.findStrategy(query.getAggregation(), config);
        strategy.initialize().ifPresent(errors::addAll);

        // Windowing Scheme is guaranteed to not be null.
        window = WindowingOperations.findScheme(query, strategy, config);
        window.initialize().ifPresent(errors::addAll);

        startTime = System.currentTimeMillis();

        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    /**
     * Consume a {@link BulletRecord} for this query. The record may or not be actually incorporated into the query
     * results. This depends on whether the query can accept more data, if it is expired or not or if the record matches
     * any query filtering criteria.
     *
     * @param record The BulletRecord to consume.
     */
    @Override
    public void consume(BulletRecord record) {
        // If query or window is closed, or does not match the filters, don't consume...
        if (areAnyClosed(this, window) || !filter(record)) {
            return;
        }
        window.consume(project(record));
    }

    /**
     * Presents the query with a serialized data representation of a prior result for the query.
     *
     * @param data The serialized data that represents a partial query result.
     */
    @Override
    public void combine(byte[] data) {
        try {
            window.combine(data);
        } catch (RuntimeException e) {
            log.error("Unable to aggregate {} for query {}", data, this);
            log.error("Skipping due to", e);
        }
    }


    /**
     * Get the result emitted so far after the last window.
     *
     * @return The byte[] representation of the serialized result.
     */
    @Override
    public byte[] getData() {
        try {
            return window.getData();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized aggregation for query {}", this);
            log.error("Skipping due to", e);
            return null;
        }
    }

    /**
     * Returns the {@link List} of {@link BulletRecord} result so far. See {@link #getResult()} for the full result
     * with metadata.
     *
     * @return The records that are part of the result.
     */
    @Override
    public List<BulletRecord> getRecords() {
        try {
            return window.getRecords();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized result for query {}", this);
            return null;
        }
    }

    /**
     * Returns the {@link Metadata} of the result so far. See {@link #getResult()} for the full result with the data.
     *
     * @return The metadata part of the result.
     */
    @Override
    public Metadata getMetadata() {
        try {
            Metadata metadata = window.getMetadata();
            metadata.merge(getResultMetadata());
            return metadata;
        } catch (RuntimeException e) {
            log.error("Unable to get metadata for query {}", this);
            return null;
        }
    }

    /**
     * Gets the resulting {@link Clip} of the results so far.
     *
     * @return A non-null {@link Clip} representing the aggregated result.
     */
    @Override
    public Clip getResult() {
        Clip result;
        try {
            result = window.getResult();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized data for query {}", this);
            result = Clip.of(Metadata.of(ParsingError.makeError(e.getMessage(), AGGREGATION_FAILURE_RESOLUTION)));
        }
        result.add(getResultMetadata());
        return result;
    }

    /**
     * Returns true if the query cannot consume any more data at this time.
     *
     * @return boolean denoting if query has expired.
     */
    @Override
    public boolean isClosed() {
        return isExpired() || isWindowClosed();
    }

    /**
     * Resets this object. You must call start or {@link #initialize()} to use it again.
     */
    @Override
    public void reset() {
        window.reset();
    }

    // ****************** Public helpers ******************

    /**
     * Returns true if the query has expired.
     *
     * @return A boolean denoting whether the query has expired.
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > startTime + query.getDuration();
    }

    /**
     * Returns true if the window is currently closed.
     *
     * @return A boolean denoting whether this window is currently closed and new data will not be accepted.
     */
    public boolean isWindowClosed() {
        return window.isClosed();
    }

    /**
     * Terminate the query and return the final result.
     *
     * @return The final non-null {@link Clip} representing the final result.
     */
    public Clip finish() {
        Metadata meta = new Metadata();
        consumeRegisteredConcept(Concept.QUERY_FINISH_TIME, (k) -> meta.add(k, System.currentTimeMillis()));
        return getResult().add(meta);
    }

    @Override
    public String toString() {
        return String.format("%s : %s", id, query.toString());
    }

    // ****************** Private helpers ******************

    private boolean filter(BulletRecord record) {
        List<Clause> filters = query.getFilters();
        // Add the record if we have no filters
        if (filters == null) {
            return true;
        }
        // Otherwise short circuit evaluate till the first filter fails. Filters are ANDed.
        return filters.stream().allMatch(c -> FilterOperations.perform(record, c));
    }

    private BulletRecord project(BulletRecord record) {
        Projection projection = query.getProjection();
        BulletRecord projected = projection != null ? ProjectionOperations.project(record, projection) : record;
        return addAdditionalFields(projected);
    }

    private Metadata getResultMetadata() {
        if (metaKeys.isEmpty()) {
            return null;
        }
        Metadata meta = new Metadata();
        consumeRegisteredConcept(Concept.QUERY_ID, (k) -> meta.add(k, id));
        consumeRegisteredConcept(Concept.QUERY_BODY, (k) -> meta.add(k, query.toString()));
        consumeRegisteredConcept(Concept.QUERY_RECEIVE_TIME, (k) -> meta.add(k, startTime));
        consumeRegisteredConcept(Concept.RESULT_EMIT_TIME, (k) -> meta.add(k, System.currentTimeMillis()));
        return meta;
    }

    private void consumeRegisteredConcept(Concept concept, Consumer<String> action) {
        Queryable.consumeRegisteredConcept(concept, metaKeys, action);
    }

    private BulletRecord addAdditionalFields(BulletRecord record) {
        if (shouldInjectTimestamp) {
            record.setLong(timestampKey, System.currentTimeMillis());
        }
        return record;
    }
}
