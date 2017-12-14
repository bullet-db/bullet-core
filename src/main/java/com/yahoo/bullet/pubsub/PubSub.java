/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.BulletConfig;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Notation: Partition is a unit of parallelism in the Pub/Sub queue.
 *
 * Implementations of PubSub should take in a {@link BulletConfig} and use the information to wire up and return
 * Publishers and Subscribers.
 */
public abstract class PubSub {
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
    protected BulletConfig config;

    /**
     * Instantiate a PubSub using parameters from {@link BulletConfig}.
     *
     * @param config The {@link BulletConfig} containing all required PubSub parameters.
     * @throws PubSubException if the context name is not present or cannot be parsed.
     */
    public PubSub(BulletConfig config) throws PubSubException {
        this.config = config;
        this.context = Context.valueOf(getRequiredConfig(String.class, BulletConfig.PUBSUB_CONTEXT_NAME));
    }

    /**
     * Get a {@link Publisher} instance wired to write to all allocated partitions in the appropriate queue (See
     * {@link PubSub#context}).
     *
     * @return {@link Publisher} wired as required.
     * @throws PubSubException if the Publisher could not be created.
     */
    public abstract Publisher getPublisher() throws PubSubException;

    /**
     * Get a list of n {@link Publisher} instances with the allocated partitions in the appropriate queue
     * (See {@link PubSub#context}) split as evenly as possible among them.
     *
     * @param n The number of Publishers requested.
     * @return The {@link List} of n Publishers wired as required.
     * @throws PubSubException if Publishers could not be created.
     */
    public abstract List<Publisher> getPublishers(int n) throws PubSubException;

    /**
     * Get a {@link Subscriber} instance wired to read from all allocated partitions in the appropriate queue (See
     * {@link PubSub#context}).
     *
     * @return {@link Subscriber} wired as required.
     * @throws PubSubException if the Subscriber could not be created.
     */
    public abstract Subscriber getSubscriber() throws PubSubException;

    /**
     * Get a list of n {@link Subscriber} instances with allocated partitions from the appropriate queue
     * (See {@link PubSub#context}) split as evenly as possible among them.
     *
     * @param n The number of Subscribers requested.
     * @return The {@link List} of n Subscribers wired as required.
     * @throws PubSubException if Subscribers could not be created.
     */
    public abstract List<Subscriber> getSubscribers(int n) throws PubSubException;

    /**
     * Create a PubSub instance using the class specified in the config file.
     *
     * @param config The {@link BulletConfig} containing the class name and PubSub settings.
     * @return an instance of specified class initialized with settings from the input file and defaults.
     * @throws PubSubException if PubSub creation fails.
     */
    public static PubSub from(BulletConfig config) throws PubSubException {
        try {
            String pubSubClassName = (String) config.get(BulletConfig.PUBSUB_CLASS_NAME);
            Class<? extends PubSub> pubSubClass = (Class<? extends PubSub>) Class.forName(pubSubClassName);
            Constructor<? extends PubSub> constructor = pubSubClass.getConstructor(BulletConfig.class);
            return constructor.newInstance(config);
        } catch (Exception e) {
            throw new PubSubException("Cannot create PubSub instance.", e);
        }
    }

    /**
     * A method to get a required configuration of a particular type.
     *
     * @param name The name of the required configuration.
     * @param clazz The class of the required configuration.
     * @param <T> The type to cast the configuration to. Inferred from tClass.
     * @return The extracted configuration of type T.
     * @throws PubSubException if the configuration is missing or cannot be cast to type T.
     */
    public <T> T getRequiredConfig(Class<T> clazz, String name) throws PubSubException {
        try {
            return config.getRequiredConfigAs(name, clazz);
        } catch (Exception e) {
            throw PubSubException.forArgument(name, e);
        }
    }
}
