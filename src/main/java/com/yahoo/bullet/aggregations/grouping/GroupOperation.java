/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.grouping;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.parsing.ParsingError;
import com.yahoo.bullet.querying.AggregationOperations;
import com.yahoo.bullet.querying.AggregationOperations.GroupOperationType;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.yahoo.bullet.parsing.ParsingError.makeError;
import static java.util.Arrays.asList;

/**
 * This class captures an operation that will be performed on an entire group - counts, sums, mins etc.
 * Other than count, all other operations include a field name on which the operation is applied.
 */
@AllArgsConstructor @Getter
public class GroupOperation implements Serializable {
    public static final long serialVersionUID = 40039294765462402L;

    public static final Set<GroupOperationType> SUPPORTED_GROUP_OPERATIONS = new HashSet<>(asList(GroupOperationType.COUNT,
                                                                                                  GroupOperationType.AVG,
                                                                                                  GroupOperationType.MAX,
                                                                                                  GroupOperationType.MIN,
                                                                                                  GroupOperationType.SUM));

    public static final String OPERATION_REQUIRES_FIELD_RESOLUTION = "Please add a field for this operation.";
    public static final String GROUP_OPERATION_REQUIRES_FIELD = "Group operation requires a field: ";
    public static final ParsingError REQUIRES_FIELD_OR_OPERATION_ERROR =
            makeError("This aggregation needs at least one field or operation", "Please add a field or valid operation.");

    public static final String OPERATIONS = "operations";
    public static final String OPERATION_TYPE = "type";
    public static final String OPERATION_FIELD = "field";
    public static final String OPERATION_NEW_NAME = "newName";

    private final GroupOperationType type;
    private final String field;
    // Ignored purposefully for hashCode and equals
    private final String newName;

    @Override
    public int hashCode() {
        // Not relying on Enum hashcode
        String typeString = type == null ? null : type.getName();
        return Objects.hash(typeString, field);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof GroupOperation)) {
            return false;
        }
        GroupOperation other = (GroupOperation) object;
        if (type != other.type) {
            return false;
        }
        if (field == null && other.field == null) {
            return true;
        }
        return field != null && field.equals(other.field);
    }

    /**
     * Returns true if the attributes contains a {@link GroupOperation#OPERATIONS} field defined.
     *
     * @param attributes The attributes that contains the operations.
     * @return A boolean denoting whether there were operations.
     */
    public static boolean hasOperations(Map<String, Object> attributes) {
        return !Utilities.isEmpty(attributes) && attributes.get(OPERATIONS) != null;
    }

    /**
     * Validates whether the provided {@link Collection} of {@link GroupOperation} is valid.
     *
     * @param operations The non-null operations to normalize.
     * @return An {@link Optional} {@link List} of {@link BulletError} if any operations were invalid or null if valid.
     */
    public static Optional<List<BulletError>> checkOperations(Collection<GroupOperation> operations) {
        List<BulletError> errors = new ArrayList<>();
        for (GroupOperation o : operations) {
            if (o.getField() == null && o.getType() != AggregationOperations.GroupOperationType.COUNT) {
                errors.add(makeError(GROUP_OPERATION_REQUIRES_FIELD + o.getType(), OPERATION_REQUIRES_FIELD_RESOLUTION));
            }
        }
        return errors.size() > 0 ? Optional.of(errors) : Optional.empty();
    }

    /**
     * Parses a {@link Set} of group operations from an Object that is expected to be a {@link List} of {@link Map}.
     *
     * @param attributes An Map that contains an object that is the representation of List of group operations.
     * @return A {@link Set} of GroupOperation or {@link Collections#emptySet()}.
     */
    @SuppressWarnings("unchecked")
    public static Set<GroupOperation> getOperations(Map<String, Object> attributes) {
        if (!hasOperations(attributes)) {
            return Collections.emptySet();
        }
        List<Object> operations = Utilities.getCasted(attributes, OPERATIONS, List.class);
        if (operations == null) {
            return Collections.emptySet();
        }
        // Return a list of distinct, non-null, GroupOperations
        return operations.stream().map(GroupOperation::makeOperation).filter(Objects::nonNull).collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private static GroupOperation makeOperation(Object object) {
        try {
            Map<String, String> data = (Map<String, String>) object;

            String type = data.get(OPERATION_TYPE);
            Optional<GroupOperationType> operation = SUPPORTED_GROUP_OPERATIONS.stream().filter(t -> t.isMe(type)).findFirst();
            // May or may not be present
            String field = data.get(OPERATION_FIELD);
            // May or may not be present
            String newName = data.get(OPERATION_NEW_NAME);
            // Unknown GroupOperations are ignored.
            return operation.isPresent() ? new GroupOperation(operation.get(), field, newName) : null;
        } catch (ClassCastException | NullPointerException e) {
            return null;
        }
    }
}
