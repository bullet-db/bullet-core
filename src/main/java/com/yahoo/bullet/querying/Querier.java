/*
 *  Copyright 201*, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Monoidal;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.windowing.Scheme;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.yahoo.bullet.common.Initializable.tryInitializing;

/**
 * This manages a {@link RunningQuery} that is currently being executed. It can {@link #consume(BulletRecord)} records for the
 * query, and {@link #combine(byte[])} serialized data from another instance of the running query. It can also merge
 * itself with another instance of running query using {@link #merge(Monoidal)}. Use {@link #finish()} to retrieve the
 * final results and terminate the query.
 *
 * If you serialize this object, you must call {@link #start()} or {@link #initialize()} before using it after
 * deserialization.
 *
 * Ideally to implement Bullet, you would parallelize into two stages:
 *
 * 1. The Filter stage - this is where you partition and distribute your data across workers. Each worker should have
 *    a copy of the query so no matter where the data ends up, the query as a whole across all the workers, will see it.
 * 2. The Join stage - this is where you group the results for all the parallelized outputs in the filter stage for a
 *    a particular query (using the query ID as an identifier to combine the intermediate outputs).
 *
 * Filter Stage Flow
 *
 * 1. For each Query message from the PubSub, check to see if it is has a KILL signal.
 *    If yes, remove any existing {@link Querier} objects for that query (identified by the ID)
 *    If no, create an instance of {@link Querier} for the query. If any exceptions, ignore them.
 * 2. For every {@link BulletRecord}, call {@link #consume(BulletRecord)} on all the {@link Querier} objects.
 * 3. If {@link #isClosedForPartition()}, use {@link #getData()} to emit the intermediate data to the Join stage for
 *    the query ID. Then, call {@link #reset()}.
 * 4. If {@link #isExpired()}, call {@link #getData()} and also remove the {@link Querier}. Can also do this periodically.
 * 5. Optional: if you are processing record by record (instead of micro-batches) and honoring
 *    {@link #isClosedForPartition()}, you should check if {@link #isExceedingRateLimit()} is true after calling
 *    {@link #getData()}. If yes, you should cancel the query and emit a signal to the Join stage to kill the query. You
 *    can use {@link #getLimiter()} and use {@link RateLimiter#getCurrentRate()} to pass the exceeded rate as well.
 *
 * You can also use {@link #haveData()} to check if there is any data to emit if you need.
 *
 * Join Stage Flow
 *
 * 1. For each Query message from the PubSub, create an instance of {@link Querier} for the query. If any exceptions,
 *    make BulletError objects from them and return them as a {@link Clip} back through the PubSub.
 * 2. For each KILL message from the Filter stage, call {@link #finish()}, and add to the {@link Meta} a
 *    {@link RateLimitError}. Emit this through the regular PubSub Publisher for results. Also emit a KILL signal
 *    PubSubMessage to a Publisher for queries so that it is fed back to the Filter stage.
 * 3.
 *
 *
 * While {@link Querier} is not serializable, you can {@link #merge(Monoidal)} it with other instances if you use
 * non-native serialization frameworks. This will simply be equivalent to calling {@link #combine(byte[])} on
 * {@link #getData()}
 */
@Slf4j
public class Querier implements Monoidal {
    public static final String AGGREGATION_FAILURE_RESOLUTION = "Please try again later";

    @Setter(AccessLevel.PACKAGE)
    private Scheme window;

    // For convenience
    @Setter(AccessLevel.PACKAGE)
    private Query query;

    private RunningQuery runningQuery;

    private BulletConfig config;
    private Map<String, String> metaKeys;
    private boolean shouldInjectTimestamp;
    private String timestampKey;
    private boolean haveData = false;

    // This is counting the number of times we get the data out of the query.
    @Getter
    private RateLimiter limiter;

    /**
     * Constructor that takes a String representation of the query and a configuration to use. This also starts the
     * query.
     *
     * @param id The query ID.
     * @param queryString The query as a string.
     * @param config The validated {@link BulletConfig} configuration to use.
     * @throws BulletException if there was an issue starting the query.
     * @throws JsonParseException if there was an issue parsing the query.
     */
    public Querier(String id, String queryString, BulletConfig config) throws JsonParseException, BulletException {
        this(new RunningQuery(id, queryString, config), config);
    }

    /**
     * Constructor that takes a {@link RunningQuery} instance and a configuration to use. This also starts executing
     * the query.
     *
     * @param query The running query.
     * @param config The validated {@link BulletConfig} configuration to use.
     * @throws BulletException if there was an issue with setting up the query.
     */
    public Querier(RunningQuery query, BulletConfig config) throws BulletException {
        this.runningQuery = query;
        this.query = query.getQuery();
        this.config = config;
        start();
    }

    /**
     * Starts the query and throws a {@link BulletException} with any errors if it was not possible to start the query.
     *
     * @return This object for chaining.
     * @throws BulletException if there were issues starting the query.
     */
    public Querier start() throws BulletException {
        tryInitializing(this);
        return this;
    }

    // ********************************* Monoidal Interface Overrides *********************************

    /**
     * Starts the query.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Optional<List<BulletError>> initialize() {
        shouldInjectTimestamp = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP, Boolean.class);
        timestampKey = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP_KEY, String.class);
        metaKeys = (Map<String, String>) config.getAs(BulletConfig.RESULT_METADATA_METRICS, Map.class);

        List<BulletError> errors = new ArrayList<>();

        runningQuery.initialize().ifPresent(errors::addAll);

        // Aggregation is guaranteed to not be null and guaranteed to have a proper type.
        Strategy strategy = AggregationOperations.findStrategy(query.getAggregation(), config);
        strategy.initialize().ifPresent(errors::addAll);

        // Windowing Scheme is guaranteed to not be null.
        window = WindowingOperations.findScheme(query, strategy, config);
        window.initialize().ifPresent(errors::addAll);

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
        // Ignore if query is expired, closed, or doesn't match filters. But consume if the window.isPartitionClosed.
        if (isExpired() || isClosed() || !filter(record)) {
            return;
        }

        BulletRecord projected = project(record);
        try {
            window.consume(projected);
            haveData = true;
        } catch (RuntimeException e) {
            log.error("Unable to consume {} for query {}", record, this);
            log.error("Skipping due to", e);
        }
    }

    /**
     * Presents the query with a serialized data representation of a prior result for the query. These will be included
     * into the query results even if the query is {@link #isClosed()} or {@link #isExpired()}.
     *
     * @param data The serialized data that represents a partial query result.
     */
    @Override
    public void combine(byte[] data) {
        try {
            window.combine(data);
            haveData = true;
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
            limiter.increment();
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
            limiter.increment();
            return window.getRecords();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized result for query {}", this);
            return null;
        }
    }

    /**
     * Returns the {@link Meta} of the result so far. See {@link #getResult()} for the full result with the data.
     *
     * @return The metadata part of the result.
     */
    @Override
    public Meta getMetadata() {
        try {
            Meta meta = window.getMetadata();
            meta.merge(getResultMetadata());
            return meta;
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
            limiter.increment();
            result = window.getResult();
        } catch (RuntimeException e) {
            log.error("Unable to get serialized data for query {}", this);
            result = Clip.of(Meta.of(BulletError.makeError(e.getMessage(), AGGREGATION_FAILURE_RESOLUTION)));
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
        return window.isClosed();
    }

    /**
     * Resets this object. You should call this if you have called {@link #getResult()} or {@link #getData()} after
     * verifying whether this is {@link #isClosed()} or {@link #isClosedForPartition()}.
     */
    @Override
    public void reset() {
        window.reset();
        haveData = false;
    }

    // ********************************* Public helpers *********************************

    /**
     * Returns true if the query has been consuming parts of the data (parallelized) and should emit the result
     * for that partition of data when operating that way. Use this if you have distributed the
     * {@link #consume(BulletRecord)} calls across multiple machines and you want to know if, for this particular kind
     * of query, whether it is necessary to emit results now. While not necessary to use, it would keep the
     * windowing semantics for the query correct to adhere to emitting when this is true.
     *
     * @return A boolean denoting whether there is data to emit for this query if it was reading part of the data.
     */
    public boolean isClosedForPartition() {
        return window.isPartitionClosed();
    }

    /**
     * Returns true if the query has expired and will never accept any more data..
     *
     * @return A boolean denoting whether the query has expired.
     */
    public boolean isExpired() {
        return window.isPermanentlyClosed() || timeIsUp();
    }

    /**
     * Returns whether there is any data to emit at all. Use this method if you are driving how data is consumed by this
     * instance (for instance, microbatches) and need to emit data outside the windowing standards.
     *
     * @return A boolean denoting whether we have any data that can be emitted.
     */
    public boolean haveData() {
        return haveData;
    }

    /**
     * Returns whether this is exceeding the rate limit. It is up to the user to perform any action such as killing the
     * query if this is
     *
     * @return A boolean denoting whether we have exceeded the rate limit.
     */
    public boolean isExceedingRateLimit() {
        // TODO: Check if rate limiting is enabled
        return limiter.isRateLimited();
    }

    /**
     * Terminate the query and return the final result.
     *
     * @return The final non-null {@link Clip} representing the final result.
     */
    public Clip finish() {
        Meta meta = new Meta();
        addMetadata(Concept.QUERY_FINISH_TIME, (k) -> meta.add(k, System.currentTimeMillis()));
        return getResult().add(meta);
    }

    @Override
    public String toString() {
        return String.format("%s : %s", runningQuery.getId(), query.toString());
    }

    // ********************************* Private helpers *********************************

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

    private BulletRecord addAdditionalFields(BulletRecord record) {
        if (shouldInjectTimestamp) {
            record.setLong(timestampKey, System.currentTimeMillis());
        }
        return record;
    }

    private Meta getResultMetadata() {
        if (metaKeys.isEmpty()) {
            return null;
        }
        Meta meta = new Meta();
        addMetadata(Concept.QUERY_ID, (k) -> meta.add(k, runningQuery.getId()));
        addMetadata(Concept.QUERY_BODY, (k) -> meta.add(k, query.toString()));
        addMetadata(Concept.QUERY_RECEIVE_TIME, (k) -> meta.add(k, runningQuery.getStartTime()));
        addMetadata(Concept.RESULT_EMIT_TIME, (k) -> meta.add(k, System.currentTimeMillis()));
        return meta;
    }

    private void addMetadata(Concept concept, Consumer<String> action) {
        Meta.consumeRegisteredConcept(concept, metaKeys, action);
    }

    private boolean timeIsUp() {
        return System.currentTimeMillis() > runningQuery.getStartTime() + query.getDuration();
    }
}
