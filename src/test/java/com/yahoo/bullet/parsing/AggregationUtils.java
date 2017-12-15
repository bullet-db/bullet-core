/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.querying.AggregationOperations;
import com.yahoo.bullet.querying.AggregationOperations.DistributionType;
import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.result.Metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class AggregationUtils {
    public static BulletConfig addMetadata(BulletConfig config, List<Map.Entry<Metadata.Concept, String>> metadata) {
        if (metadata == null) {
            config.set(BulletConfig.RESULT_METADATA_ENABLE, false);
            return config;
        }
        List<Map<String, String>> metadataList = new ArrayList<>();
        for (Map.Entry<Metadata.Concept, String> meta : metadata) {
            Map<String, String> entry = new HashMap<>();
            entry.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, meta.getKey().getName());
            entry.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, meta.getValue());
            metadataList.add(entry);
        }
        config.set(BulletConfig.RESULT_METADATA_METRICS, metadataList);
        config.set(BulletConfig.RESULT_METADATA_ENABLE, true);
        return config;
    }

    @SafeVarargs
    public static BulletConfig addMetadata(BulletConfig config, Map.Entry<Metadata.Concept, String>... metadata) {
        return addMetadata(config, metadata == null ? null : Arrays.asList(metadata));
    }

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

    public static Map<String, String> makeGroupOperation(GroupOperation operation) {
        return makeGroupOperation(operation.getType(), operation.getField(), operation.getNewName());
    }

    public static Map<String, String> makeGroupOperation(AggregationOperations.GroupOperationType type, String field, String newName) {
        Map<String, String> map = new HashMap<>();
        if (type != null) {
            map.put(GroupOperation.OPERATION_TYPE, type.getName());
        }
        if (field != null) {
            map.put(GroupOperation.OPERATION_FIELD, field);
        }
        if (newName != null) {
            map.put(GroupOperation.OPERATION_NEW_NAME, newName);
        }
        return map;
    }

    @SafeVarargs
    public static Map<String, Object> makeAttributes(Map<String, String>... maps) {
        return makeAttributes(asList(maps));
    }

    public static Map<String, Object> makeAttributes(List<Map<String, String>> maps) {
        Map<String, Object> map = new HashMap<>();
        map.put(GroupOperation.OPERATIONS, maps);
        return map;
    }

    public static Map<String, Object> makeAttributes(DistributionType type, Double start, Double end, Double increment,
                                                     Long numberOfPoints, List<Double> points) {
        Map<String, Object> map = new HashMap<>();
        if (type != null) {
            map.put(Distribution.TYPE, type.getName());
        }
        if (start != null && end != null && increment != null) {
            map.put(Distribution.RANGE_START, start);
            map.put(Distribution.RANGE_END, end);
            map.put(Distribution.RANGE_INCREMENT, increment);
        }
        if (numberOfPoints != null) {
            map.put(Distribution.NUMBER_OF_POINTS, numberOfPoints);
        }
        if (points != null) {
            map.put(Distribution.POINTS, points);
        }
        return map;
    }

    public static Map<String, Object> makeAttributes(DistributionType type, double start, double end, double increment) {
        return makeAttributes(type, start, end, increment, null, null);
    }

    public static Map<String, Object> makeAttributes(DistributionType type, long numberOfPoints) {
        return makeAttributes(type, null, null, null, numberOfPoints, null);
    }

    public static Map<String, Object> makeAttributes(DistributionType type, List<Double> points) {
        return makeAttributes(type, null, null, null, null, points);
    }

    public static Map<String, Object> makeAttributes(String name) {
        Map<String, Object> map = new HashMap<>();
        if (name != null) {
            map.put(CountDistinct.NEW_NAME_FIELD, name);
        }
        return map;
    }

    public static Map<String, Object> makeAttributes(String name, Long threshold) {
        Map<String, Object> map = new HashMap<>();
        if (threshold != null) {
            map.put(TopK.THRESHOLD_FIELD, threshold);
        }
        if (name != null) {
            map.put(TopK.NEW_NAME_FIELD, name);
        }
        return map;
    }
}
