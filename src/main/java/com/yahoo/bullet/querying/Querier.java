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
import com.yahoo.bullet.common.Monoidal;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.windowing.Basic;
import com.yahoo.bullet.windowing.Scheme;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * This manages a {@link RunningQuery} that is currently being executed. It can {@link #consume(BulletRecord)} records for the
 * query, and {@link #combine(byte[])} serialized data from another instance of the running query. It can also merge
 * itself with another instance of running query using {@link #merge(Monoidal)}. Use {@link #finish()} to retrieve the
 * final results and terminate the query.
 *
 * <p>After you create this object, you <strong>must call</strong> {@link #initialize()} before using it.</p>
 *
 * <p>Ideally to implement Bullet, you would parallelize into two stages:</p>
 *
 * <ol>
 * <li>
 *  <strong>The Filter Stage</strong> - this is where you partition and distribute your data across workers. Each worker
 *  should have a copy of the query so no matter where the data ends up, the query as a whole across all the workers,
 *  will see it.
 * </li>
 * <li>
 * <strong>The Join Stage</strong> - this is where you group the results for all the parallelized outputs in the filter
 * stage for a particular query (using the query ID as an identifier to combine the intermediate outputs).
 * </li>
 * </ol>
 *
 * <h3>Filter Stage</h3>
 *
 * <ol>
 * <li>
 *   For each Query message from the PubSub, check to see if it is has a KILL or COMPLETE signal.
 *   If yes, remove any existing {@link Querier} objects for that query (identified by the ID)
 *   If no, create an instance of {@link Querier} for the query. If any exceptions or errors initializing, throw away
 *   the querier.
 * </li>
 * <li>
 *   For every {@link BulletRecord}, call {@link #consume(BulletRecord)} on all the {@link Querier} objects
 *   unless {@link #isDone()}
 * </li>
 * <li>
 *   If {@link #isDone()}, call {@link #getData()} and also remove the {@link Querier}.
 * </li>
 * <li>
 *   If {@link #isClosedForPartition()}, use {@link #getData()} to emit the intermediate data to the Join stage for
 *   the query ID. Then, call {@link #reset()}.
 * </li>
 * <li>
 *   <em>Optional</em>: if you are processing record by record (instead of micro-batches) and honoring {@link #isClosedForPartition()},
 *   you should check if {@link #isExceedingRateLimit()} is true after calling {@link #getData()}. If yes, you should
 *   cancel the query and emit a RateLimitError to the Join stage to kill the query. You can use {@link #getRateLimitError()}
 *   to get the {@link RateLimitError} to pass to the Join stage.
 * </li>
 * <li>
 *   <em>Optional</em>: If your data volume is very, very small (Heuristic: less than 1 per your 0.1 *
 *   bullet.query.window.min.emit.every). across your partitions), you should run the {@link #isDone()} and
 *   {@link #isClosedForPartition()} and do the emits either on a timer or at fixed intervals so that your queries
 *   are checked for results and maintain their windowing guarantees.
 * </li>
 * </ol>
 *
 * You can also use {@link #hasData()} to check if there is any data to emit if you need. If you do not want to call
 * {@link #getData()}, you can serialize Querier using non-native serialization frameworks and use {@link #merge(Monoidal)}
 * in the Join stage to merge them into an empty Querier for the query. This will be equivalent to calling
 * {@link #combine(byte[])} on {@link #getData()}. Just remember to not call {@link #initialize()}
 * on the reified querier objects on the Join side since that will wipe the existing results stored in them.
 *
 * <h4>Pseudo Code</h4>
 *
 * <h5>Case 1: Query</h5>
 *
 * <pre>
 * (String id, String queryBody, Metadata metadata) = Query
 * if (metadata.hasSignal(Signal.KILL) || metadata.hasSignal(Signal.COMPLETE))
 *     remove Querier for id
 * else
 *     create new Querier(id, queryBody, config) and initialize it;
 * </pre>
 *
 * <h5>Case 2: BulletRecord record</h5>
 *
 * <pre>
 * for Querier q : queriers:
 *     if (q.isDone())
 *         emit(q.getData())
 *         remove q
 *     else
 *         if (q.isClosedForPartition())
 *             emit(q.getData())
 *             q.reset()
     *         q.consume(record)
 *         if (q.isExceedingRateLimit())
 *             emit(q.getRateLimitError())
 *             remove q
 * </pre>
 *
 * <h5>Case 3: Periodically (for very small data volumes)</h5>
 *
 * <pre>
 * for Querier q : queriers:
 *     if (q.isDone())
 *         emit(q.getData())
 *         remove q
 *     if (q.isClosedForPartition())
 *         emit(q.getData())
 *         q.reset()
 *     if (q.isExceedingRateLimit())
 *         emit(q.getRateLimitError())
 *         remove q
 * </pre>
 *
 * <h3>Join Stage</h3>
 *
 * <ol>
 * <li>
 *   For each Query message from the PubSub, if it is a KILL signal similar to the Filter stage, kill the query and
 *   return. Otherwise create an instance of {@link Querier} for the query. If any exceptions or errors initializing it,
 *   make BulletError objects from them and return them as a {@link Clip} back through the PubSub.
 * </li>
 * <li>
 *   For each KILL message from the Filter stage, call {@link #finish()}, and add to the {@link Meta} a
 *   {@link RateLimitError}. Emit this through the regular PubSub Publisher for results. Also emit a KILL signal
 *   PubSubMessage to a Publisher for queries so that it is fed back to the Filter stage.
 * </li>
 * <li>
 *   For each (id, byte[]) data from the Filter stage, call {@link #combine(byte[])} for the querier for that id.
 * </li>
 * <li>
 *   If {@link #isDone()}, call {@link #getResult()} to emit the final result and remove the querier. Emit a
 *   COMPLETE signal to the PubSub Publisher for queries to feed back the complete status to the Filter stage.
 * </li>
 * <li>
 *   If {@link #isClosed}, use {@link #getResult()} ()} to emit the intermediate result and call {@link #reset()}
 * </li>
 * <li>
 *   <em>Optional</em>: as with the Filter stage, if the querier {@link #isExceedingRateLimit()}, you can use
 *   {@link #getRateLimitError()} and then {@link RateLimitError#makeMeta()} to create and emit a
 *   {@link Clip#add(Meta)}. Make sure to remove the querier and send a KILL signal to a PubSub Publisher for queries
 *   to feed back the kill status to the Filter stage.
 * </li>
 * <li>
 *   <em>Optional</em>: Similar to the filter stage you should run {@link #isDone()} and {@link #isClosed()} periodically if your
 *   data volume is too low.
 * </li>
 * <li>
 *   <em>Optional but recommended if you are processing event by event not microbatches</em>: Since data from the Filter stage
 *   partitions may not arrive at the same time and since the querier may be {@link #isClosed()} at the same time the
 *   data from the Filter stage partitions arrive, you should not immediately emit {@link #getResult()} if
 *   {@link #isClosed()} and then {@link #reset()} for <em>non time-based windows</em>. You should put it in a buffer
 *   for a little bit of time while the data arrives if {@link #isClosed()}, then {@link #getResult()} and
 *   {@link #reset()}. Similarly, for {@link #isDone()}. You should only do this for queries for which
 *   {@link #isTimeBasedWindow()} is true. For record based windows, you can use {@link #isClosed()} to drive the
 *   emission of the results.
 * </li>
 * </ol>
 *
 * <h4>Pseudo Code</h4>
 *
 * <h5>Case 1:  Query</h5>
 * <pre>
 * (String id, String queryBody, Metadata metadata) = Query
 * if (metadata.hasSignal(Signal.KILL))
 *     remove Querier for id
 *     return
 * try {
 *     create new Querier(id, queryBody, config)
 *     initialize it and if errors present:
 *         emit(Clip.of(Meta.of(errors.get()));
 * catch (Exception e) {
 *     Clip clip = Clip.of(Meta.of(asList(BulletError.makeError(e, queryBody)))
 *     emit(clip)
 * </pre>
 *
 *
 * <h5>Case 2: KILL messages from Filter</h5>
 *
 * <pre>
 * (String id, RateLimitError error) = KILL message
 *
 * querier = Querier for id
 * Clip clip = querier.finish()
 * clip.add(Meta.of(error))
 * emit(clip)
 * queryPubSubPublisher.emit((id, KILL signal))
 * remove querier
 * </pre>
 *
 * <h5>Case 3: Data message from Filter</h5>
 *
 * <pre>
 * (String id, byte[] data) = Data
 *
 * querier = Querier for id
 * querier.combine(data)
 * if (querier.isDone())
 *     Clip clip = querier.finish()
 *     emit(clip)
 *     queryPubSubPublisher.emit(
 * else if (querier.isClosed())
 *     Clip clip = querier.getResult()
 *     // See note above regarding buffering if querier.isTimeBasedWindow()
 *     emit(clip)
 *     querier.reset()
 *
 * if (querier.isExceedingRateLimit())
 *     Clip clip = merge q.finish() with q.getRateLimitError()
 *     emit(clip)
 *     queryPubSubPublisher.emit((id, KILL signal))
 *     remove q
 * </pre>
 *
 * <h5>Case 4: Periodically (for very small data volumes)</h5>
 *
 * <pre>
 * for Querier q : queriers:
 *     if (q.isDone())
 *         emit(q.finish())
 *         remove q
 *     if (q.isClosed())
 *         emit(q.getResult())
 *         q.reset()
 *     if (q.isExceedingRateLimit())
 *         Clip clip = merge q.finish() with q.getRateLimitError()
 *         emit(clip)
 *         queryPubSubPublisher.emit((id, KILL signal))
 *         remove q
 * </pre>
 */
@Slf4j
public class Querier implements Monoidal {
    public static final String TRY_AGAIN_LATER = "Please try again later";

    // For testing convenience
    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private Scheme window;

    // For testing convenience
    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private RunningQuery runningQuery;

    private BulletConfig config;
    private Map<String, String> metaKeys;
    private String timestampKey;
    private boolean hasData = false;

    // This is counting the number of times we get the data out of the query.
    private RateLimiter rateLimit;

    /**
     * Constructor that takes a String representation of the query and a configuration to use. This also starts the
     * query.
     *
     * @param id The query ID.
     * @param queryString The query as a string.
     * @param config The validated {@link BulletConfig} configuration to use.
     * @throws JsonParseException if there was an issue parsing the query.
     */
    public Querier(String id, String queryString, BulletConfig config) throws JsonParseException {
        this(new RunningQuery(id, queryString, config), config);
    }

    /**
     * Constructor that takes a {@link RunningQuery} instance and a configuration to use. This also starts executing
     * the query.
     *
     * @param query The running query.
     * @param config The validated {@link BulletConfig} configuration to use.
     */
    public Querier(RunningQuery query, BulletConfig config) {
        this.runningQuery = query;
        this.config = config;
    }

    // ********************************* Monoidal Interface Overrides *********************************

    /**
     * Starts the query.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Optional<List<BulletError>> initialize() {
        // Is an empty map if metadata was disabled
        metaKeys = (Map<String, String>) config.getAs(BulletConfig.RESULT_METADATA_METRICS, Map.class);

        Boolean shouldInjectTimestamp = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP, Boolean.class);
        if (shouldInjectTimestamp) {
            timestampKey = config.getAs(BulletConfig.RECORD_INJECT_TIMESTAMP_KEY, String.class);
        }

        boolean isRateLimitEnabled = config.getAs(BulletConfig.RATE_LIMIT_ENABLE, Boolean.class);
        if (isRateLimitEnabled) {
            int maxEmit = config.getAs(BulletConfig.RATE_LIMIT_MAX_EMIT_COUNT, Integer.class);
            int timeInterval = config.getAs(BulletConfig.RATE_LIMIT_TIME_INTERVAL, Integer.class);
            rateLimit = new RateLimiter(maxEmit, timeInterval);
        }

        Optional<List<BulletError>> errors;

        errors = runningQuery.initialize();
        if (errors.isPresent()) {
            return errors;
        }

        Query query = this.runningQuery.getQuery();

        // Aggregation and Strategy are guaranteed to not be null.
        Strategy strategy = AggregationOperations.findStrategy(query.getAggregation(), config);
        errors = strategy.initialize();
        if (errors.isPresent()) {
            return errors;
        }

        // Scheme is guaranteed to not be null.
        window = WindowingOperations.findScheme(query, strategy, config);
        return window.initialize();
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
        // Ignore if query is expired, or doesn't match filters. But consume if the window is closed (partition or otherwise)
        if (isDone() || !filter(record)) {
            return;
        }

        BulletRecord projected = project(record);
        try {
            window.consume(projected);
            hasData = true;
        } catch (RuntimeException e) {
            log.error("Unable to consume {} for query {}", record, this);
            log.error("Skipping due to", e);
        }
    }

    /**
     * Presents the query with a serialized data representation of a prior result for the query. These will be included
     * into the query results even if the query is {@link #isClosed()} or {@link #isDone()}.
     *
     * @param data The serialized data that represents a partial query result.
     */
    @Override
    public void combine(byte[] data) {
        try {
            window.combine(data);
            hasData = true;
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
            incrementRate();
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
            incrementRate();
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
        Meta meta;
        try {
            meta = window.getMetadata();
            meta.merge(getResultMetadata());
        } catch (RuntimeException e) {
            log.error("Unable to get metadata for query {}", this);
            meta = getErrorMeta(e);
        }
        return meta;
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
            incrementRate();
            result = window.getResult();
            result.add(getResultMetadata());
        } catch (RuntimeException e) {
            log.error("Unable to get serialized data for query {}", this);
            result = Clip.of(getErrorMeta(e));
        }
        return result;
    }

    /**
     * Returns true if the query window is closed and you should emit the result at this time. If you have partitioned
     * the data, use {@link #isClosedForPartition()}.
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
        hasData = false;
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
        return window.isClosedForPartition();
    }

    /**
     * Returns true if the query has expired and will never accept any more data.
     *
     * @return A boolean denoting whether the query has expired.
     */
    public boolean isDone() {
        // We're done with the query if this is the last window and it is closed or query has timed out.
        return (isLastWindow() && window.isClosed()) || runningQuery.isTimedOut();
    }

    /**
     * Returns whether there is any data to emit at all. Use this method if you are driving how data is consumed by this
     * instance (for instance, microbatches) and need to emit data outside the windowing standards.
     *
     * @return A boolean denoting whether we have any data that can be emitted.
     */
    public boolean hasData() {
        return hasData;
    }

    /**
     * Returns whether this is exceeding the rate limit. It is up to the user to perform any action such as killing the
     * query if this is
     *
     * @return A boolean denoting whether we have exceeded the rate limit.
     */
    public boolean isExceedingRateLimit() {
        return rateLimit != null && rateLimit.isRateLimited();
    }

    /**
     * Returns a {@link RateLimitError} if the rate limit had exceeded the rate from a prior call to
     * {@link #isExceedingRateLimit()}.
     *
     * @return A rate limit error or null if the rate limit was not exceeded.
     */
    public RateLimitError getRateLimitError() {
        if (rateLimit == null || !rateLimit.isExceededRate()) {
            return null;
        }
        return new RateLimitError(rateLimit.getCurrentRate(), config);
    }

    /**
     * Returns if this query has a time based window.
     *
     * @return A boolean that is true if this is a time based query window.
     */
    public boolean isTimeBasedWindow() {
        Window window = runningQuery.getQuery().getWindow();
        boolean noWindow = window == null;
        // If it's a RAW query without a window, it is NOT time based.
        if (noWindow && isRaw()) {
            return false;
        }
        // No window means duration drives the query, it IS time based. Otherwise, check if the window is time based.
        return noWindow || window.isTimeBased();
    }

    /**
     * Terminate the query and return the final result.
     *
     * @return The final non-null {@link Clip} representing the final result.
     */
    public Clip finish() {
        Clip result = getResult();
        Meta meta = new Meta();
        addMetadata(Concept.QUERY_FINISH_TIME, (k) -> meta.add(k, System.currentTimeMillis()));
        result.add(meta);
        return result;
    }

    @Override
    public String toString() {
        return runningQuery.toString();
    }

    // ********************************* Private helpers *********************************

    private boolean filter(BulletRecord record) {
        List<Clause> filters = runningQuery.getQuery().getFilters();
        // Add the record if we have no filters
        if (filters == null) {
            return true;
        }
        // Otherwise short circuit evaluate till the first filter fails. Filters are ANDed.
        return filters.stream().allMatch(c -> FilterOperations.perform(record, c));
    }

    private BulletRecord project(BulletRecord record) {
        Projection projection = runningQuery.getQuery().getProjection();
        BulletRecord projected = projection != null ? ProjectionOperations.project(record, projection) : record;
        return addAdditionalFields(projected);
    }

    private BulletRecord addAdditionalFields(BulletRecord record) {
        if (timestampKey != null) {
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
        addMetadata(Concept.QUERY_BODY, (k) -> meta.add(k, runningQuery.getQuery().toString()));
        addMetadata(Concept.QUERY_RECEIVE_TIME, (k) -> meta.add(k, runningQuery.getStartTime()));
        addMetadata(Concept.RESULT_EMIT_TIME, (k) -> meta.add(k, System.currentTimeMillis()));
        return meta;
    }

    private void addMetadata(Concept concept, Consumer<String> action) {
        Meta.consumeRegisteredConcept(concept, metaKeys, action);
    }

    private boolean isLastWindow() {
        // For now, we only need this to work for Basic windows (i.e. no windows) to quickly terminate queries that
        // have no windows. In the future, this could be computed using window attributes and duration.
        return window.getClass().equals(Basic.class);
    }

    private boolean isRaw() {
        return runningQuery.getQuery().getAggregation().getType() == Aggregation.Type.RAW;
    }

    private Meta getErrorMeta(Exception e) {
        return Meta.of(BulletError.makeError(e.getMessage(), TRY_AGAIN_LATER));
    }

    private void incrementRate() {
        if (rateLimit != null) {
            rateLimit.increment();
        }
    }

}
