/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class ProjectionUtils {
    public static Projection makeProjection(String field, String newName) {
        return makeProjection(singletonMap(field, newName));
    }

    public static Projection makeProjection(Pair<String, String>... entries) {
        Map<String, String> fields = new HashMap<>();
        for (Pair<String, String> entry : entries) {
            fields.put(entry.getKey(), entry.getValue());
        }
        return makeProjection(fields);
    }

    public static Projection makeProjection(Map<String, String> fields) {
        Projection projection = new Projection();
        projection.setFields(fields);
        return projection;
    }
}
