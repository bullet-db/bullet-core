/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class RESTPubSubConfig extends BulletConfig {
    // Field names
    public static final String PREFIX = "bullet.pubsub.rest.";
    public static final String CONNECT_TIMEOUT_MS = PREFIX + "connect.timeout.ms";
    public static final String CONNECT_RETRY_LIMIT = PREFIX + "connect.retry.limit";
    public static final String MAX_UNCOMMITTED_MESSAGES = PREFIX + "subscriber.max.uncommitted.messages";
    public static final String QUERY_URLS = PREFIX + "query.urls";
    public static final String RESULT_URL = PREFIX + "result.url";
    public static final String RESULT_SUBSCRIBER_MIN_WAIT = PREFIX + "result.subscriber.min.wait.ms";
    public static final String QUERY_SUBSCRIBER_MIN_WAIT = PREFIX + "query.subscriber.min.wait.ms";

    // Defaults
    public static final Integer DEFAULT_CONNECT_TIMEOUT_MS = 30000;
    public static final Integer DEFAULT_CONNECT_RETRY_LIMIT = 10;
    public static final Integer DEFAULT_MAX_UNCOMMITTED_MESSAGES = 100;
    public static final List<String> DEFAULT_QUERY_URLS = Arrays.asList("http://localhost:9901/bullet/api/pubsub/query",
                                                                        "http://localhost:9902/bullet/api/pubsub/query");
    public static final String DEFAULT_RESULT_URL = "http://localhost:9901/bullet/api/pubsub/result";
    public static final Long DEFAULT_RESULT_MIN_WAIT = 10L;
    public static final Long DEFAULT_QUERY_MIN_WAIT = 10L;

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     */
    public RESTPubSubConfig(String file) {
        this(new BulletConfig(file));
    }

    /**
     * Constructor that loads the defaults and augments it with defaults.
     *
     * @param other The other config to wrap.
     */
    public RESTPubSubConfig(Config other) {
        super();
        merge(other);
        VALIDATOR.validate(this);
        log.info("Merged settings:\n {}", this);
    }

    private static final Validator VALIDATOR = BulletConfig.getValidator();
    static {
        VALIDATOR.define(CONNECT_TIMEOUT_MS)
                 .defaultTo(DEFAULT_CONNECT_TIMEOUT_MS)
                 .checkIf(Validator::isPositive);
        VALIDATOR.define(CONNECT_RETRY_LIMIT)
                 .defaultTo(DEFAULT_CONNECT_RETRY_LIMIT)
                 .checkIf(Validator::isPositive);
        VALIDATOR.define(MAX_UNCOMMITTED_MESSAGES)
                 .defaultTo(DEFAULT_MAX_UNCOMMITTED_MESSAGES)
                 .checkIf(Validator::isPositive);
        VALIDATOR.define(QUERY_URLS)
                 .defaultTo(DEFAULT_QUERY_URLS)
                 .checkIf(Validator::isList);
        VALIDATOR.define(RESULT_URL)
                 .defaultTo(DEFAULT_RESULT_URL)
                 .checkIf(Validator::isString);
        VALIDATOR.define(RESULT_SUBSCRIBER_MIN_WAIT)
                 .defaultTo(DEFAULT_RESULT_MIN_WAIT)
                 .checkIf(Validator::isPositive);
        VALIDATOR.define(QUERY_SUBSCRIBER_MIN_WAIT)
                 .defaultTo(DEFAULT_QUERY_MIN_WAIT)
                 .checkIf(Validator::isPositive);
    }

    @Override
    public BulletConfig validate() {
        super.validate(this);
        VALIDATOR.validate(this);
        return this;
    }
}
