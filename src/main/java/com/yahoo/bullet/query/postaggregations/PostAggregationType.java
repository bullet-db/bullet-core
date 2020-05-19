/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

/**
 * Represents the type of the PostAggregation.
 */
public enum PostAggregationType {
    HAVING,
    COMPUTATION,
    ORDER_BY,
    CULLING;
}
