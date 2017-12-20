/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.aggregations.Strategy;
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
import com.yahoo.bullet.result.Metadata.Concept;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * This manages a {@link Query} that is currently being executed. It can {@link #consume(BulletRecord)} records for the
 * query, and {@link #merge(byte[])} serialized data from another instance of the running query. It can also merge itself
 * with another instance of running query using {@link #merge(Querier)}.
 */
@Slf4j
public class Querier implements Serializable {
    public static final String AGGREGATION_FAILURE_RESOLUTION = "Please try again later";

    private String id;
    @Setter(AccessLevel.PACKAGE)
    private Query query;

    @Getter @Setter
    private long startTime;

    private Boolean shouldInjectTimestamp;
    private String timestampKey;


    // Deliberately not serialized
    @Setter(AccessLevel.PACKAGE)
    private transient Strategy strategy;

    private Map<String, String> metadataKeys;

    /**
     * Constructor that takes a {@link Query} instance and a configuration to use. This also starts the query.
     *
     * @param id The query ID.
     * @param query The query object.
     * @param config The validated {@link BulletConfig} configuration to use.
     * @throws ParsingException if there was an issue with setting up the query.
     */
    public Querier(String id, Query query, BulletConfig config) throws ParsingException {
        this.id = id;
        this.query = query;
        start(config);
    }

    /**
     * Constructor that takes a String representation of the query and a configuration to use. This also starts the
     * query.
     *
     * @param id The query ID.
     * @param queryString The query as a string.
     * @param config The validated {@link BulletConfig} configuration to use.
     * @throws ParsingException if there was an issue.
     */
    public Querier(String id, String queryString, BulletConfig config) throws JsonParseException, ParsingException {
        this(id, Parser.parse(queryString, config), config);
    }

    /**
     * Starts the query. You must call this method if you have deserialized an instance of this Object from data.
     *
     * @param config A {@link BulletConfig} to initialize the query with.
     * @return This object for chaining.
     * @throws ParsingException
     */
    @SuppressWarnings("unchecked")
    public Querier start(BulletConfig config) throws ParsingException {
        shouldInjectTimestamp = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP, Boolean.class);
        timestampKey = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP_KEY, String.class);
        metadataKeys = (Map<String, String>) config.getAs(BulletConfig.RESULT_METADATA_METRICS, Map.class);

        Optional<List<Error>> errors = query.initialize();
        if (errors.isPresent()) {
            throw new ParsingException(errors.get());
        }
        // Aggregation is guaranteed to not be null and guaranteed to have a proper type
        strategy = AggregationOperations.findStrategy(query.getAggregation(), config);
        errors = strategy.initialize();
        if (errors.isPresent()) {
            throw new ParsingException(errors.get());
        }
        startTime = System.currentTimeMillis();
        return this;
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
        if (isExpired() || !strategy.isAcceptingData() || !filter(record)) {
            return false;
        }
        aggregate(project(record));
        return isMicroBatch();
    }

    /**
     * Presents the query with a serialized data representation of a prior result for the query.
     *
     * @param data The serialized data that represents a partial query result.
     * @return A boolean denoting whether this instance will not accept any more data.
     */
    public boolean merge(byte[] data) {
        try {
            strategy.combine(data);
        } catch (RuntimeException e) {
            log.error("Unable to aggregate {} for query {}", data, this);
            log.error("Skipping due to", e);
        }
        // If the strategy is no longer accepting data, then the Query has been satisfied.
        return !strategy.isAcceptingData();
    }

    /**
     * Merge the data in another instance with this one. This only merges the data or results for the query and not any
     * metadata about the running query.
     *
     * @param querier The other instance to merge into this one.
     * @return A boolean denoting whether this instance will not accept any more data.
     */
    public boolean merge(Querier querier) {
        return merge(querier.getData());
    }

    /**
     * Get the result emitted so far after the last window.
     *
     * @return The byte[] representation of the serialized result.
     */
    public byte[] getData() {
        try {
            return strategy.getSerializedAggregation();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized aggregation for query {}", this);
            log.error("Skipping due to", e);
            return null;
        }
    }

    /**
     * Gets the resulting {@link Clip} of the results so far.
     *
     * @return A non-null {@link Clip} representing the aggregated result.
     */
    public Clip getResult() {
        Clip result;
        try {
            result = strategy.getAggregation();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized aggregation for query {}", this);
            log.error("Skipping due to", e);
            result = Clip.of(Metadata.of(Error.makeError(e.getMessage(), AGGREGATION_FAILURE_RESOLUTION)));
        }
        result.add(getResultMetadata(id));
        return result;
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

    /**
     * Returns true if the query has expired.
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

    private void aggregate(BulletRecord record) {
        try {
            strategy.consume(record);
        } catch (RuntimeException e) {
            log.error("Unable to consume {} for query {}", record, this);
            log.error("Skipping due to", e);
        }
    }

    private Metadata getResultMetadata(String id) {
        if (metadataKeys.isEmpty()) {
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
        // Only consume the concept if we have a key for it: i.e. it was registered
        String key = metadataKeys.get(concept.getName());
        if (key != null) {
            action.accept(key);
        }
    }

    private boolean isMicroBatch() {
        // TODO: Windowing
        return true;
    }

    private BulletRecord addAdditionalFields(BulletRecord record) {
        if (shouldInjectTimestamp) {
            record.setLong(timestampKey, System.currentTimeMillis());
        }
        return record;
    }
}

