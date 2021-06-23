/*
 *  Copyright 2021 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Query;

import java.io.Serializable;

/**
 * This allows you to hook into the {@link PubSubMessage} serialization and deserialization - for instance, if you want
 * to customize what is done to the message before it is sent to or read in the backend. You can implement a custom
 * version of this to change what the {@link PubSubMessage} contains (see {@link #toMessage(PubSubMessage)}) and invert
 * it when it is read back (see {@link #fromMessage(PubSubMessage)}).
 *
 * Please note that this is not intended to be applied to messages read from the backend or for signals sent to the
 * backend. You can, for example, use this if you want to customize what is stored in storage or to lazily create a
 * {@link Query} in the backend.
 */
public abstract class PubSubMessageSerDe implements Serializable {
    private static final long serialVersionUID = 5352288558800960763L;

    protected BulletConfig config;

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to configure this class.
     */
    public PubSubMessageSerDe(BulletConfig config) {
        this.config = config;
    }

    /**
     * A helper to submit a query by converting it to a standard {@link PubSubMessage} and running it through
     * {@link #toMessage(PubSubMessage)}.
     *
     * @param id The ID for the query.
     * @param query The {@link Query} object to create a {@link PubSubMessage}
     * @param queryString The BQL for the query.
     * @return A converted {@link PubSubMessage}.
     */
    public PubSubMessage toMessage(String id, Query query, String queryString) {
        return toMessage(new PubSubMessage(id, query, new Metadata(null, queryString)));
    }

    /**
     * Takes a {@link PubSubMessage} and returns the new format of the message. Note that it is allowed to modify the
     * original message. See {@link #fromMessage(PubSubMessage)} for the inverse.
     *
     * @param message The {@link PubSubMessage} to convert.
     * @return A converted {@link PubSubMessage}.
     */
    public abstract PubSubMessage toMessage(PubSubMessage message);

    /**
     * Takes a converted {@link PubSubMessage} and returns the original format of the message. Note that it is allowed
     * to modify the original message. See {@link #toMessage(PubSubMessage)} for the inverse.
     *
     * @param message The {@link PubSubMessage} to revert.
     * @return A reverted {@link PubSubMessage}.
     */
    public abstract PubSubMessage fromMessage(PubSubMessage message);

    /**
     * Create a {@link PubSubMessageSerDe} instance using the class specified in the config file.
     *
     * @param config The non-null {@link BulletConfig} containing the class name and its settings.
     * @return An instance of specified class initialized with settings from the input file and defaults.
     */
    public static PubSubMessageSerDe from(BulletConfig config) {
        try {
            return config.loadConfiguredClass(BulletConfig.PUBSUB_MESSAGE_SERDE_CLASS_NAME);
        } catch (RuntimeException e) {
            throw new RuntimeException("Cannot create PubSubMessageSerDe instance.", e.getCause());
        }
    }
}
