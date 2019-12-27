/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Validator;

import java.util.Collections;
import java.util.Map;

public class HTTPMetricPublisherConfig extends BulletConfig {
    private static final String HTTP_NAMESPACE = "bullet.metrics.publisher.http.";
    // The URL to use for HTTP publishing
    public static final String URL = HTTP_NAMESPACE + "url";
    // The name of the metrics group being used by this instance
    public static final String GROUP = HTTP_NAMESPACE + "metric.group";
    // The names to values of the standard static dimensions to use for all requests
    public static final String DIMENSIONS = HTTP_NAMESPACE + "dimensions";
    // The number of times to retry a particular request. A request will always be tried once
    public static final String RETRIES = HTTP_NAMESPACE + "retries";
    // The time between each retry attempt in milliseconds
    public static final String RETRY_INTERVAL_MS = HTTP_NAMESPACE + "retry.interval.ms";
    // The maximum concurrent connections to be used for HTTP requests
    public static final String MAX_CONCURRENCY = HTTP_NAMESPACE + "max.concurrency";

    public static final String DEFAULT_GROUP = "default";
    public static final Map<String, String> DEFAULT_DIMENSIONS = Collections.emptyMap();
    public static final int DEFAULT_RETRIES = 1;
    public static final int DEFAULT_RETRY_INTERVAL_MS = 1000;
    public static final int DEFAULT_MAX_CONCURRENCY = 20;

    private static final long serialVersionUID = -4709952650452653204L;
    private static final Validator VALIDATOR = BulletConfig.getValidator();
    static {
        VALIDATOR.define(URL)
                 .checkIf(Validator::isString)
                 .castTo(Validator::asString)
                 .orFail();
        VALIDATOR.define(GROUP)
                 .defaultTo(DEFAULT_GROUP)
                 .checkIf(Validator::isString)
                 .castTo(Validator::asString);
        VALIDATOR.define(DIMENSIONS)
                 .defaultTo(DEFAULT_DIMENSIONS)
                 .checkIf(Validator.isMapOfType(String.class, String.class));
        VALIDATOR.define(RETRIES)
                 .defaultTo(DEFAULT_RETRIES)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);
        VALIDATOR.define(RETRY_INTERVAL_MS)
                 .defaultTo(DEFAULT_RETRY_INTERVAL_MS)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);
        VALIDATOR.define(MAX_CONCURRENCY)
                 .defaultTo(DEFAULT_MAX_CONCURRENCY)
                 .checkIf(Validator::isPositiveInt)
                 .castTo(Validator::asInt);
    }

    /**
     * Create an instance of this configuration.
     *
     * @param config Another configuration to merge.
     */
    public HTTPMetricPublisherConfig(BulletConfig config) {
        super(null);
        merge(config);
    }

    @Override
    public BulletConfig validate() {
        VALIDATOR.validate(this);
        return this;
    }
}
