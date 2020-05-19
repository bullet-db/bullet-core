/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

/** Represents the type of the Aggregation. */
public enum AggregationType {
    GROUP,
    COUNT_DISTINCT,
    TOP_K,
    DISTRIBUTION,
    RAW
}
