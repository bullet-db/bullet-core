/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.aggregations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class AggregationUtils {
    public static Map<String, String> makeGroupFields(List<String> fields) {
        if (fields != null) {
            return fields.stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
        }
        return new HashMap<>();
    }

    public static Map<String, String> makeGroupFields(String... fields) {
        if (fields == null) {
            return new HashMap<>();
        }
        return makeGroupFields(asList(fields));
    }
}
