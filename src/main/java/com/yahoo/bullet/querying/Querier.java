/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.tablefunctions.TableFunction;
import com.yahoo.bullet.querying.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Monoidal;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.querying.postaggregations.PostStrategy;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import com.yahoo.bullet.querying.tablefunctors.TableFunctor;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.bullet.windowing.Basic;
import com.yahoo.bullet.windowing.Scheme;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.yahoo.bullet.query.Projection.Type.COPY;
import static com.yahoo.bullet.query.Projection.Type.PASS_THROUGH;
import static com.yahoo.bullet.result.Meta.addIfNonNull;

/**
 * This manages a {@link RunningQuery} that is currently being executed. It can {@link #consume(BulletRecord)} records for the
 * query, and {@link #combine(byte[])} serialized data from another instance of the running query. It can also merge
 * itself with another instance of running query using {@link #merge(Monoidal)}. Use {@link #finish()} to retrieve the
 * final results and terminate the query.
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
 *   If no, create an instance of {@link Querier} for the query in {@link Mode#PARTITION} mode if and only if you are
 *   going to be persisting the querier for the duration of the query. If you are throwing away the querier, such as
 *   after processing your partitioned data in mini-batches and recreating it every new mini-batch, then you need not
 *   change the mode. If any exceptions or errors initializing, throw away the querier since the errors are handled in
 *   the Join stage below.
 * </li>
 * <li>
 *   For every {@link BulletRecord}, call {@link #consume(BulletRecord)} on all the {@link Querier} objects
 *   unless {@link #isDone()}
 * </li>
 * <li>
 *   If {@link #isDone()}, call {@link #getData()} and also remove the {@link Querier}.
 * </li>
 * <li>
 *   If {@link #isClosed()}, use {@link #getData()} to emit the intermediate data to the Join stage for
 *   the query ID. Then, call {@link #reset()}.
 * </li>
 * <li>
 *   <em>Optional</em>: if you are processing record by record (instead of micro-batches) and honoring {@link #isClosed()},
 *   you should check if {@link #isExceedingRateLimit()} is true after calling {@link #getData()}. If yes, you should
 *   cancel the query and emit a RateLimitError to the Join stage to kill the query. You can use {@link #getRateLimitError()}
 *   to get the {@link RateLimitError} to pass to the Join stage.
 * </li>
 * <li>
 *   <em>Optional</em>: If your data volume is very, very small (Heuristic: less than 1 per your 0.1 *
 *   bullet.query.window.min.emit.every.ms). across your partitions), you should run the {@link #isDone()} and
 *   {@link #isClosed()} and do the emits either on a timer or at fixed intervals so that your queries
 *   are checked for results and maintain their windowing guarantees.
 * </li>
 * </ol>
 *
 * You can also use {@link #hasNewData()} to check if there is any new data to emit if you need to know a successful
 * consumption or combining happened.
 *
 * If you do not want to call {@link #getData()}, you can serialize Querier using non-native serialization frameworks
 * and use {@link #merge(Monoidal)} in the Join stage to merge them into an empty Querier for the query. This will be
 * equivalent to calling {@link #combine(byte[])} on {@link #getData()}.
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
 *         if (q.isClosed())
 *             emit(q.getData())
 *             q.reset()
 *             q.consume(record)
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
 *     if (q.isClosed())
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
 *   return. Otherwise create an instance of {@link Querier} for the query in {@link Mode#ALL} mode. If any exceptions
 *   or errors initializing it,  make BulletError objects from them and return them as a {@link Clip} back through the PubSub.
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
 *   <em>Optional Delayed start and End (recommended if you are processing event by event)</em>: Since data from the
 *   Filter stage partitions may not arrive at the same time and since the querier may be {@link #isClosed()} at the
 *   same time the data from the Filter stage partitions arrive, you should not immediately emit {@link #getResult()} if
 *   {@link #isClosed()} and then {@link #reset()}. There are two ways to handle this. You could delay the start of the
 *   query by a bit in the Join stage so that windows from the Filter stage always arrive a bit earlier. Or you could
 *   buffer the results in the Join stage for a bit for each window as results trickle in. The issue with the latter
 *   approach is that you will slowly add the buffer time to the duration of your windows in your Join stage and
 *   eventually you will get two windows in one. The former approach does not have this problem. However, that approach
 *   could lead to results that are sent immediately (for record based windows) being dropped while the delay is
 *   happening. To solve these issues, you should buffer the final results for all queries for whom {@link #shouldBuffer()}
 *   is true. This should be true for time-based windows and false for all record-based windows or queries with no
 *   window. So you can use the negation of {@link #shouldBuffer()} to find out if the latter queries can be delayed.
 *   This delay will ensure that results from the filter phase are collected in their entirety before emitting from the
 *   Join phase. To aid you in doing this, you can buffer it and use {@link #restart()} to mark the delayed start of the
 *   query.
 * </li>
 * </ol>
 *
 * <h4>Pseudo Code</h4>
 *
 * <h5>Case 1: Query</h5>
 * <pre>
 * (String id, String queryBody, Metadata metadata) = Query
 * if (metadata.hasSignal(Signal.KILL))
 *     remove Querier for id
 *     return
 * try {
 *     create new Querier(id, queryBody, config)
 *     initialize it (see note above regarding delaying start) and if errors present:
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
 *     // See note above regarding buffering if querier.shouldBuffer()
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
    /**
     * This is used to determine if this operates in partitioned mode or not. If the Querier is operating in
     * {@link Mode#PARTITION}, it is assumed there are multiple queriers running in parallel and consuming parts of the
     * data for the query. Use this if you are distributing the {@link #consume(BulletRecord)} calls across multiple
     * machines. This fixes the semantics of the {@link #reset()} and the {@link #isClosed()} methods to keep the
     * correct windowing semantics.
     *
     * If you are not distributing the data or recreating the querier instance in your parallelized step, you can
     * leave this at the default of {@link Mode#ALL}.
     */
    public enum Mode {
        PARTITION, ALL
    }

    public static final String TRY_AGAIN_LATER = "Please try again later";

    // For testing convenience
    @Getter(AccessLevel.PACKAGE) @Setter(AccessLevel.PACKAGE)
    private Scheme window;

    @Getter
    private RunningQuery runningQuery;

    private Filter filter;

    private TableFunctor tableFunctor;

    private Projection projection;

    // Transient field, DO NOT use it beyond constructor and initialize methods.
    private transient BulletConfig config;

    private Map<String, String> metaKeys;
    private boolean hasNewData = false;

    // This is counting the number of times we get the data out of the query.
    private RateLimiter rateLimit;

    // Mode for the querier
    private Mode mode;

    private List<PostStrategy> postStrategies;

    private BulletRecordProvider provider;

    /**
     * Constructor that takes a {@link RunningQuery} instance and a configuration to use. This also starts executing
     * the query.
     *
     * @param query The running query.
     * @param config The validated {@link BulletConfig} configuration to use.
     */
    public Querier(RunningQuery query, BulletConfig config) {
        this(Mode.ALL, query, config);
    }

    /**
     * Constructor that takes a {@link Querier.Mode}, {@link RunningQuery} instance and a configuration to use.
     * This also starts executing the query.
     *
     * @param mode The mode for this querier.
     * @param query The running query.
     * @param config The validated {@link BulletConfig} configuration to use.
     */
    public Querier(Mode mode, RunningQuery query, BulletConfig config) {
        this.mode = mode;
        this.runningQuery = query;
        this.config = config;
        this.provider = config.getBulletRecordProvider();
        start();
    }

    // ********************************* Monoidal Interface Overrides *********************************

    /**
     * Starts the query.
     */
    private void start() {
        // Is an empty map if metadata was disabled
        metaKeys = (Map<String, String>) config.getAs(BulletConfig.RESULT_METADATA_METRICS, Map.class);

        boolean isRateLimitEnabled = config.getAs(BulletConfig.RATE_LIMIT_ENABLE, Boolean.class);
        if (isRateLimitEnabled) {
            int maxEmit = config.getAs(BulletConfig.RATE_LIMIT_MAX_EMIT_COUNT, Integer.class);
            int timeInterval = config.getAs(BulletConfig.RATE_LIMIT_TIME_INTERVAL, Integer.class);
            rateLimit = new RateLimiter(maxEmit, timeInterval);
        }

        Query query = runningQuery.getQuery();

        Expression filter = query.getFilter();
        if (filter != null) {
            this.filter = new Filter(filter);
        }

        TableFunction tableFunction = query.getTableFunction();
        if (tableFunction != null) {
            tableFunctor = tableFunction.getTableFunctor();
        }

        com.yahoo.bullet.query.Projection projection = query.getProjection();
        if (projection.getType() != PASS_THROUGH) {
            this.projection = new Projection(projection.getFields());
        }

        // Aggregation and Strategy are guaranteed to not be null.
        Strategy strategy = query.getAggregation().getStrategy(config);

        List<PostAggregation> postAggregations = query.getPostAggregations();
        if (postAggregations != null && !postAggregations.isEmpty()) {
            postStrategies = postAggregations.stream().map(PostAggregation::getPostStrategy).collect(Collectors.toList());
        }

        // Scheme is guaranteed to not be null. It is constructed in its "start" state.
        window = query.getWindow().getScheme(strategy, config);
    }

    /**
     * Forces a restart of a valid query to mark the
     * correct start of this object if it was previously created but delayed in starting it (by using the negation of
     * {@link #shouldBuffer()}. You might be using this if you were delaying the start of the query in the
     * Join phase. This does not revalidate the query or reset any data this might have already consumed.
     */
    public void restart() {
        // Currently, only necessary to mark the correct start of Tumbling and AdditiveTumbling windows.
        window.start();
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
        // Ignore if query is expired. But consume if the window is closed (partition or otherwise)
        if (isDone()) {
            return;
        }
        consumeRecord(record);
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
            hasNewData = true;
        } catch (RuntimeException e) {
            log.error("Unable to aggregate {} for query {}", data, this);
            log.error("Skipping due to", e);
        }
    }

    /**
     * Get the result emitted so far after the last window. Post aggregations are NOT applied.
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
            Clip result = new Clip();
            result.add(window.getRecords());
            result = postAggregate(result);
            result = outerQuery(result);
            return result.getRecords();
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
            result = postAggregate(result);
            result = outerQuery(result);
            result.add(getResultMetadata());
        } catch (RuntimeException e) {
            log.error("Unable to get serialized data for query {}", this);
            result = Clip.of(getErrorMeta(e));
        }
        return result;
    }

    /**
     * Depending on the {@link Mode#ALL} mode this is operating in, returns true if and only if `the query window is
     * closed and you should emit the result at this time.
     *
     * @return boolean denoting if query has closed.
     */
    @Override
    public boolean isClosed() {
        return mode == Mode.PARTITION ? window.isClosedForPartition() : window.isClosed();
    }

    /**
     * Resets this object. You should call this if you have called {@link #getResult()} or {@link #getData()} after
     * verifying whether this is {@link #isClosed()}.
     */
    @Override
    public void reset() {
        if (mode == Mode.PARTITION) {
            window.resetForPartition();
        } else {
            window.reset();
        }
        hasNewData = false;
    }

    // ********************************* Public helpers *********************************

    /**
     * Gets the {@link Query} instance for this Query.
     *
     * @return The {@link Query} instance for this object.
     */
    public Query getQuery() {
        return runningQuery.getQuery();
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
     * Returns whether there is any new data to emit at all since the last {@link #reset()}. Use this method if you are
     * driving how data is consumed by this instance (for instance, microbatches) and need to emit data outside the
     * windowing standards.
     *
     * @return A boolean denoting whether we have any new data that can be emitted.
     */
    public boolean hasNewData() {
        return hasNewData;
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
        return new RateLimitError(rateLimit.getCurrentRate(), rateLimit.getAbsoluteRateLimit());
    }

    /**
     * Returns if this query should buffer before emitting the final results. You can use this to wait for the final
     * results in your Join or Combine stage after a query is {@link #isDone()}.
     *
     * @return A boolean that is true if the query results should be buffered in the Join phase.
     */
    public boolean shouldBuffer() {
        // Only buffer if the window is not time based (RawStrategy or if it's a record based window).
        return !runningQuery.getQuery().getWindow().isTimeBased();
    }

    /**
     * Terminate the query and return the final result.
     *
     * @return The final non-null {@link Clip} representing the final result.
     */
    public Clip finish() {
        Clip result = getResult();
        addFinishTime(result.getMeta());
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s : %s", runningQuery.getId(), runningQuery.toString());
    }

    // ********************************* Private helpers *********************************

    private void consumeRecord(BulletRecord record) {
        if (tableFunctor == null) {
            process(record);
        } else {
            tableFunctor.apply(record, provider).forEach(this::process);
        }
    }

    private void process(BulletRecord record) {
        // Ignore if record doesn't match filters
        if (!filter(record)) {
            return;
        }
        try {
            BulletRecord projected = project(record);
            window.consume(projected);
            hasNewData = true;
        } catch (RuntimeException e) {
            log.error("Unable to consume {} for query {}", record, this);
            log.error("Skipping due to", e);
        }
    }

    private boolean filter(BulletRecord record) {
        if (filter == null) {
            return true;
        }
        return filter.match(record);
    }

    private BulletRecord project(BulletRecord record) {
        if (projection == null) {
            return record;
        } else if (runningQuery.getQuery().getProjection().getType() == COPY) {
            return projection.project(record.copy());
        } else {
            return projection.project(record, provider);
        }
    }

    private Clip postAggregate(Clip clip) {
        if (postStrategies == null) {
            return clip;
        }
        for (PostStrategy postStrategy : postStrategies) {
            clip = postStrategy.execute(clip);
        }
        return clip;
    }

    private Clip outerQuery(Clip clip) {
        if (runningQuery.getQuery().getOuterQuery() == null) {
            return clip;
        }
        Querier querier = new Querier(Mode.ALL, new RunningQuery(runningQuery.getId(), runningQuery.getQuery().getOuterQuery(), new Metadata()), config);
        for (BulletRecord record : clip.getRecords()) {
            // A bit inefficient since this is only needed for RAW aggregation queries
            if (querier.isClosed()) {
                break;
            }
            querier.consumeRecord(record);
        }
        Clip result = querier.getResult();
        result.getMeta().add(getInnerQueryMetaKey(), clip.getMeta().asMap());
        return result;
    }

    private Meta getResultMetadata() {
        String metaKey = getMetaKey();
        if (metaKey == null) {
            return null;
        }
        Map<String, Object> meta = new HashMap<>();
        addIfNonNull(meta, metaKeys, Concept.QUERY_ID, runningQuery::getId);
        addIfNonNull(meta, metaKeys, Concept.QUERY_OBJECT, runningQuery::toString);
        addIfNonNull(meta, metaKeys, Concept.QUERY_STRING, runningQuery::getQueryString);
        addIfNonNull(meta, metaKeys, Concept.QUERY_RECEIVE_TIME, runningQuery::getStartTime);
        return new Meta().add(metaKey, meta);
    }

    private void addFinishTime(Meta meta) {
        Map<String, Object> queryMeta = (Map<String, Object>) meta.asMap().get(getMetaKey());
        if (queryMeta != null) {
            addIfNonNull(queryMeta, metaKeys, Concept.QUERY_FINISH_TIME, System::currentTimeMillis);
        }
    }

    private boolean isLastWindow() {
        // For now, we only need this to work for Basic windows (i.e. no windows) to quickly terminate queries that
        // have no windows. In the future, this could be computed using window attributes and duration.
        return window.getClass().equals(Basic.class);
    }

    private Meta getErrorMeta(Exception e) {
        return Meta.of(new BulletError(e.getMessage(), TRY_AGAIN_LATER));
    }

    private void incrementRate() {
        if (rateLimit != null) {
            rateLimit.increment();
        }
    }

    private String getMetaKey() {
        return metaKeys.getOrDefault(Concept.QUERY_METADATA.getName(), null);
    }

    private String getInnerQueryMetaKey() {
        return metaKeys.getOrDefault(Concept.INNER_QUERY_METADATA.getName(), null);
    }
}
