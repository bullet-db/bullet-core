/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.grouping;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperator;
import com.yahoo.bullet.record.BulletRecord;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.yahoo.bullet.common.Utilities.extractFieldAsNumber;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.AVG;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT_FIELD;

/**
 * This class represents the results of a GroupOperations. The result is always a {@link Number}, so
 * that is what this class stores. It is {@link Serializable}.
 *
 * It can compute all the operations if presented with a {@link BulletRecord}, merge other GroupData and
 * present the results of the operations as a BulletRecord.
 */
@Slf4j
public class GroupData implements Serializable {
    public static final long serialVersionUID = 387461949277948303L;

    public static final String NAME_SEPARATOR = "_";

    @Setter
    protected Map<String, String> groupFields;
    protected Map<GroupOperation, Number> metrics;

    /**
     * Creates a {@link Map} of {@link GroupOperation} to their numeric metric values from a {@link Set} of
     * {@link GroupOperation}.
     *
     * @param operations A set of operations.
     * @return A empty map of metrics that represent these operations.
     */
    public static Map<GroupOperation, Number> makeInitialMetrics(Set<GroupOperation> operations) {
        Map<GroupOperation, Number> metrics = new HashMap<>();
        // Initialize with nulls.
        for (GroupOperation operation : operations) {
            metrics.put(operation, null);
            if (operation.getType() == AVG) {
                // For AVG we store an addition COUNT_FIELD operation to store the count (the sum is stored in AVG)
                metrics.put(new GroupOperation(COUNT_FIELD, operation.getField(), null), null);
            }
        }
        return metrics;
    }

    /**
     * Constructor that initializes the GroupData with a {@link Set} of {@link GroupOperation} and a {@link Map} of
     * Strings that represent the group fields.
     *
     * @param groupFields The mappings of field names to their values that represent this group.
     * @param operations  the non-null operations that this will compute metrics for.
     */
    public GroupData(Map<String, String> groupFields, Set<GroupOperation> operations) {
        this.groupFields = groupFields;
        this.metrics = makeInitialMetrics(operations);
    }

    /**
     * Constructor that initializes the GroupData with a {@link Set} of {@link GroupOperation}.
     *
     * @param operations the non-null operations that this will compute metrics for.
     */
    public GroupData(Set<GroupOperation> operations) {
        this(null, operations);
    }

    /**
     * Constructor that initializes the GroupData with an existing {@link Map} of {@link GroupOperation} to values and
     * a {@link Map} of Strings that represent the group fields. These arguments are not copied.
     *
     * @param groupFields The mappings of field names to their values that represent this group.
     * @param metrics     the non-null {@link Map} of metrics for this object.
     */
    public GroupData(Map<String, String> groupFields, Map<GroupOperation, Number> metrics) {
        this.groupFields = groupFields;
        this.metrics = metrics;
    }

    /**
     * Consumes the given {@link BulletRecord} and computes group operation metrics.
     *
     * @param data The record to compute metrics for.
     */
    public void consume(BulletRecord data) {
        metrics.entrySet().stream().forEach(e -> consume(e, data));
    }

    /**
     * Merges the serialized form of a GroupData into this. For all GroupOperations present, their corresponding
     * values will be merged according to their respective additive operation.
     *
     * @param serializedGroupData the serialized bytes of a GroupData.
     */
    public void combine(byte[] serializedGroupData) {
        GroupData otherMetric = SerializerDeserializer.fromBytes(serializedGroupData);
        if (otherMetric == null) {
            log.error("Could not create a GroupData. Skipping...");
            return;
        }
        combine(otherMetric);
    }

    /**
     * Merge a GroupData into this. For all GroupOperations present, their corresponding values will be
     * merged according to their respective additive operation.
     *
     * @param otherData The other GroupData to merge.
     */
    public void combine(GroupData otherData) {
        metrics.entrySet().stream().forEach(e -> combine(e, otherData));
    }

    /**
     * Gets the metrics stored for the group as a {@link BulletRecord}.
     *
     * @return A non-null {@link BulletRecord} containing the data stored in this object.
     */
    public BulletRecord getMetricsAsBulletRecord() {
        BulletRecord record = new BulletRecord();
        metrics.entrySet().stream().forEach(e -> addToRecord(e, record));
        return record;
    }

    /**
     * Gets the metrics and the group values stored as a {@link BulletRecord}.
     *
     * @return A non-null {@link BulletRecord} containing the data stored in this object.
     */
    public BulletRecord getAsBulletRecord() {
        return getAsBulletRecord(Collections.emptyMap());
    }

    /**
     * Gets the metrics and the group values stored as a {@link BulletRecord}.
     *
     * @param mapping An non-null new name mapping for the names of the group fields.
     * @return A non-null {@link BulletRecord} containing the data stored in this object.
     */
    public BulletRecord getAsBulletRecord(Map<String, String> mapping) {
        BulletRecord record = getMetricsAsBulletRecord();
        for (Map.Entry<String, String> e : groupFields.entrySet()) {
            String field = e.getKey();
            String mapped = mapping.get(field);
            record.setString(Utilities.isEmpty(mapped) ? field : mapped, e.getValue());
        }
        return record;
    }

    private void consume(Map.Entry<GroupOperation, Number> metric, BulletRecord data) {
        GroupOperation operation = metric.getKey();
        GroupOperation.GroupOperationType type = operation.getType();

        Number casted = 1L;
        switch (type) {
            case COUNT:
                break;
            case MIN:
            case MAX:
            case SUM:
            case AVG:
                casted = extractFieldAsNumber(operation.getField(), data);
                break;
            case COUNT_FIELD:
                casted = extractFieldAsNumber(operation.getField(), data) ;
                casted = casted != null ? 1L : null;
                break;
        }
        updateMetric(casted, metric, GroupOperation.OPERATORS.get(type));
    }

    private void combine(Map.Entry<GroupOperation, Number> metric, GroupData otherData) {
        GroupOperation operation = metric.getKey();
        Number value = otherData.metrics.get(metric.getKey());
        switch (operation.getType()) {
            case MIN:
                updateMetric(value, metric, GroupOperation.MIN);
                break;
            case MAX:
                updateMetric(value, metric, GroupOperation.MAX);
                break;
            case SUM:
            case AVG:
                updateMetric(value, metric, GroupOperation.SUM);
                break;
            case COUNT:
            case COUNT_FIELD:
                updateMetric(value, metric, GroupOperation.COUNT);
                break;
        }
    }

    private void addToRecord(Map.Entry<GroupOperation, Number> metric, BulletRecord record) {
        GroupOperation operation = metric.getKey();
        Number value = metric.getValue();
        switch (operation.getType()) {
            case COUNT:
                record.setLong(getResultName(operation), value == null ? 0 : value.longValue());
                break;
            case AVG:
                record.setDouble(getResultName(operation), calculateAvg(value, operation.getField()));
                break;
            case COUNT_FIELD:
                // Internal use only for AVG. Not exposed.
                break;
            case MIN:
            case MAX:
            case SUM:
                record.setDouble(getResultName(operation), value == null ? null : value.doubleValue());
                break;
        }
    }

    private Double calculateAvg(Number sum, String field) {
        Number count = metrics.get(new GroupOperation(COUNT_FIELD, field, null));
        if (sum == null || count == null) {
            return null;
        }
        return sum.doubleValue() / count.longValue();
    }

    /**
     * Returns the name of the result field to use for the given {@link GroupOperation}. If the operation
     * does specify a newName, it will be returned. Otherwise, a composite name containing the type of the
     * operation as well as the field name will be used (if provided).
     *
     * @param operation The operation to get the name for.
     * @return a String representing a name for the result of the operation.
     */
    public static String getResultName(GroupOperation operation) {
        String name = operation.getNewName();
        if (name != null) {
            return name;
        }
        GroupOperation.GroupOperationType type = operation.getType();
        String field = operation.getField();
        if (field == null) {
            return type.getName();
        }
        return type.getName() + NAME_SEPARATOR + operation.getField();
    }

    /*
     * This function accepts an GroupOperator and applies it to the new and current value for the given
     * GroupOperation and updates metrics accordingly.
     */
    private void updateMetric(Number number, Map.Entry<GroupOperation, Number> metric, GroupOperator operator) {
        if (number == null) {
            return;
        }
        Number current = metric.getValue();
        metrics.put(metric.getKey(), current == null ? number : operator.apply(number, current));
    }
}

