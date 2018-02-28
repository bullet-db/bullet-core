/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.memory;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;
import lombok.extern.slf4j.Slf4j;
import java.util.Optional;

@Slf4j
public class MemoryPubSubConfig extends BulletConfig {
    public static final String PREFIX = "bullet.pubsub.memory.";
    // The servlet context path for the in memory pubsub rest endpoints ("api/bullet" by default)
    public static final String CONTEXT_PATH = PREFIX + "context.path";
    // The location (host:port) of this in-memory pubsub instance - used for writeQuery, writeResponse and readResponse
    public static final String SERVER = PREFIX + "server";
    // The timeout and retry limits for HTTP connections to in-memory pubsub server
    public static final String CONNECT_TIMEOUT_MS = PREFIX + "connect.timeout.ms";
    public static final String CONNECT_RETRY_LIMIT = PREFIX + "connect.retry.limit";
    // The maximum number of allowed uncommitted messages
    public static final String MAX_UNCOMMITTED_MESSAGES = PREFIX + "subscriber.max.uncommitted.messages";
    // The paths (not including the context.path) of the endpoints for reading/writing queries/responses
    public static final String READ_QUERY_PATH = PREFIX + "read.query.path";
    public static final String READ_RESPONSE_PATH = PREFIX + "read.response.path";
    public static final String WRITE_QUERY_PATH = PREFIX + "write.query.path";
    public static final String WRITE_RESPONSE_PATH = PREFIX + "write.response.path";
    // The full paths (comma-seperated list) of the http endpoints for reading queries (the backend reads from all in-memory pubsub instances)
    public static final String BACKED_READ_QUERY_PATHS = PREFIX + "backend.read.query.path";

    // Defaults
    public static final String DEFAULT_CONTEXT_PATH = "/api/bullet";
    public static final String DEFAULT_SERVER = "http://localhost:9999";
    public static final Integer DEFAULT_CONNECT_TIMEOUT_MS = 30000;
    public static final Integer DEFAULT_CONNECT_RETRY_LIMIT = 10;
    public static final Integer DEFAULT_MAX_UNCOMMITTED_MESSAGES = 100;
    public static final String DEFAULT_READ_QUERY_PATH = "/pubsub/read/query";
    public static final String DEFAULT_READ_RESPONSE_PATH = "/pubsub/read/response";
    public static final String DEFAULT_WRITE_QUERY_PATH = "/pubsub/write/query";
    public static final String DEFAULT_WRITE_RESPONSE_PATH = "/pubsub/write/response";
    public static final String DEFAULT_BACKED_READ_QUERY_PATHS = "http://localhost:9999/api/bullet/pubsub/read/query,http://localhost:9998/api/bullet/pubsub/read/query";

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     */
    public MemoryPubSubConfig(String file) {
        this(new BulletConfig(file));
    }

    /**
     * Constructor that loads the defaults and augments it with defaults.
     *
     * @param other The other config to wrap.
     */
    public MemoryPubSubConfig(Config other) {
        super();
        merge(other);
        VALIDATOR.validate(this);
        log.info("Merged settings:\n {}", getAll(Optional.empty()));
    }

    private static final Validator VALIDATOR = new Validator();
    static {
        VALIDATOR.define(CONTEXT_PATH)
                 .defaultTo(DEFAULT_CONTEXT_PATH)
                 .checkIf(Validator::isString);
        VALIDATOR.define(SERVER)
                 .defaultTo(DEFAULT_SERVER)
                 .checkIf(Validator::isString);
        VALIDATOR.define(CONNECT_TIMEOUT_MS)
                 .defaultTo(DEFAULT_CONNECT_TIMEOUT_MS)
                 .checkIf(Validator::isPositive);
        VALIDATOR.define(CONNECT_RETRY_LIMIT)
                 .defaultTo(DEFAULT_CONNECT_RETRY_LIMIT)
                 .checkIf(Validator::isPositive);
        VALIDATOR.define(MAX_UNCOMMITTED_MESSAGES)
                 .defaultTo(DEFAULT_MAX_UNCOMMITTED_MESSAGES)
                 .checkIf(Validator::isPositive);
        VALIDATOR.define(READ_QUERY_PATH)
                 .defaultTo(DEFAULT_READ_QUERY_PATH)
                 .checkIf(Validator::isString);
        VALIDATOR.define(READ_RESPONSE_PATH)
                 .defaultTo(DEFAULT_READ_RESPONSE_PATH)
                 .checkIf(Validator::isString);
        VALIDATOR.define(WRITE_QUERY_PATH)
                 .defaultTo(DEFAULT_WRITE_QUERY_PATH)
                 .checkIf(Validator::isString);
        VALIDATOR.define(WRITE_RESPONSE_PATH)
                 .defaultTo(DEFAULT_WRITE_RESPONSE_PATH)
                 .checkIf(Validator::isString);
        VALIDATOR.define(BACKED_READ_QUERY_PATHS)
                 .defaultTo(DEFAULT_BACKED_READ_QUERY_PATHS)
                 .checkIf(Validator::isString);
    }

}
