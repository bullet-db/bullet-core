package com.yahoo.bullet.pubsub;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Notation: Partition is a unit of parallelism in the Pub/Sub queue.
 *
 * Implementations of PubSub should take in a {@link PubSubConfig} and use the information to wire up and return
 * Publishers and Subscribers.
 */
public abstract class PubSub implements Serializable {
    /**
     * The context determines how the {@link Publisher} and {@link Subscriber} returned by PubSub behave. For example,
     * If the Context is {@link Context#QUERY_SUBMISSION}:
     *      1. Publishers write to request queue.
     *      2. Subscribers read from the response queue.
     * If the Context is {@link Context#QUERY_PROCESSING}:
     *      1. Publishers write to response queue.
     *      2. Subscribers read from the request queue.
     */
    public enum Context {
        QUERY_SUBMISSION,
        QUERY_PROCESSING
    }

    protected Context context;

    /**
     * Instantiate a PubSub using parameters from {@link PubSubConfig}.
     *
     * @param config The {@link PubSubConfig} containing all required PubSub parameters.
     */
    public PubSub(PubSubConfig config) {
        context = Context.valueOf(config.get(PubSubConfig.CONTEXT_NAME).toString());
    }

    /**
     * Get a {@link Publisher} instance wired to write to all allocated partitions in the appropriate queue (See
     * {@link PubSub#context}).
     *
     * @return {@link Publisher} wired as required.
     */
    public abstract Publisher getPublisher();

    /**
     * Get a list of n {@link Publisher} instances with the allocated partitions in the appropriate queue
     * (See {@link PubSub#context}) split as evenly as possible among them.
     *
     * @param n The number of Publishers requested.
     * @return The {@link List} of n Publishers wired as required.
     */
    public abstract List<Publisher> getPublishers(int n);

    /**
     * Get a {@link Subscriber} instance wired to read from all allocated partitions in the appropriate queue (See
     * {@link PubSub#context}).
     *
     * @return The {@link Subscriber} wired as required.
     */
    public abstract Subscriber getSubscriber();

    /**
     * Get a list of n {@link Subscriber} instances with allocated partitions from the appropriate queue
     * (See {@link PubSub#context}) split as evenly as possible among them.
     *
     * @param n The number of Subscribers requested.
     * @return The {@link List} of n Subscribers wired as required.
     */
    public abstract List<Subscriber> getSubscribers(int n);

    /**
     * Create a PubSub instance using the class specified in the config file.
     *
     * @param config The {@link PubSubConfig} containing the class name and PubSub settings.
     * @return an instance of specified class initialized with settings from the input file and defaults.
     * @throws PubSubException if PubSub creation fails.
     */
    public static PubSub from(PubSubConfig config) throws PubSubException {
        try {
            String pubSubClassName = (String) config.get(PubSubConfig.PUBSUB_CLASS_NAME);
            Class<? extends PubSub> pubSubClass = (Class<? extends PubSub>) Class.forName(pubSubClassName);
            Constructor<? extends PubSub> constructor = pubSubClass.getConstructor(PubSubConfig.class);
            return constructor.newInstance(config);
        } catch (Exception e) {
            throw new PubSubException("Cannot create PubSub instance. Error: " + e.toString());
        }
    }
}
