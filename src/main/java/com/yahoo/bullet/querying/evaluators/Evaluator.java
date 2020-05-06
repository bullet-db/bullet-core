/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.EnumMap;
import java.util.Map;

/**
 * Evaluators do the work of expressions.
 *
 * Each expression is built into a corresponding evaluator. Note, evaluators are constructed after a query has been
 * initialized so assume all expressions are valid.
 *
 * Evaluators are evaluated given a BulletRecord and will throw exceptions on any errors. These errors are virtually all
 * from some form of type check.
 *
 * Also, note the type cast in evaluator. For primitives, this acts as how you think it would, but for lists and maps, it
 * will cast their elements/values.
 */
public abstract class Evaluator {
    // For testing only
    @Getter(AccessLevel.PACKAGE)
    protected Type type;

    static final Map<Operation, BinaryOperations.BinaryOperator> BINARY_OPERATORS = new EnumMap<>(Operation.class);
    static final Map<Operation, BinaryOperations.BinaryOperator> BINARY_ANY_OPERATORS = new EnumMap<>(Operation.class);
    static final Map<Operation, BinaryOperations.BinaryOperator> BINARY_ALL_OPERATORS = new EnumMap<>(Operation.class);
    static final Map<Operation, UnaryOperations.UnaryOperator> UNARY_OPERATORS = new EnumMap<>(Operation.class);
    static final Map<Operation, NAryOperations.NAryOperator> N_ARY_OPERATORS = new EnumMap<>(Operation.class);

    static {
        BINARY_OPERATORS.put(Operation.ADD, BinaryOperations.ADD);
        BINARY_OPERATORS.put(Operation.SUB, BinaryOperations.SUB);
        BINARY_OPERATORS.put(Operation.MUL, BinaryOperations.MUL);
        BINARY_OPERATORS.put(Operation.DIV, BinaryOperations.DIV);
        BINARY_OPERATORS.put(Operation.EQUALS, BinaryOperations.EQUALS);
        BINARY_OPERATORS.put(Operation.NOT_EQUALS, BinaryOperations.NOT_EQUALS);
        BINARY_OPERATORS.put(Operation.GREATER_THAN, BinaryOperations.GREATER_THAN);
        BINARY_OPERATORS.put(Operation.LESS_THAN, BinaryOperations.LESS_THAN);
        BINARY_OPERATORS.put(Operation.GREATER_THAN_OR_EQUALS, BinaryOperations.GREATER_THAN_OR_EQUALS);
        BINARY_OPERATORS.put(Operation.LESS_THAN_OR_EQUALS, BinaryOperations.LESS_THAN_OR_EQUALS);
        BINARY_OPERATORS.put(Operation.REGEX_LIKE, BinaryOperations.REGEX_LIKE);
        BINARY_OPERATORS.put(Operation.SIZE_IS, BinaryOperations.SIZE_IS);
        BINARY_OPERATORS.put(Operation.CONTAINS_KEY, BinaryOperations.CONTAINS_KEY);
        BINARY_OPERATORS.put(Operation.CONTAINS_VALUE, BinaryOperations.CONTAINS_VALUE);
        BINARY_OPERATORS.put(Operation.IN, BinaryOperations.IN);
        BINARY_OPERATORS.put(Operation.AND, BinaryOperations.AND);
        BINARY_OPERATORS.put(Operation.OR, BinaryOperations.OR);
        BINARY_OPERATORS.put(Operation.XOR, BinaryOperations.XOR);
        BINARY_OPERATORS.put(Operation.FILTER, BinaryOperations.FILTER);

        BINARY_ANY_OPERATORS.put(Operation.EQUALS, BinaryOperations.EQUALS_ANY);
        BINARY_ANY_OPERATORS.put(Operation.NOT_EQUALS, BinaryOperations.NOT_EQUALS_ANY);
        BINARY_ANY_OPERATORS.put(Operation.GREATER_THAN, BinaryOperations.GREATER_THAN_ANY);
        BINARY_ANY_OPERATORS.put(Operation.LESS_THAN, BinaryOperations.LESS_THAN_ANY);
        BINARY_ANY_OPERATORS.put(Operation.GREATER_THAN_OR_EQUALS, BinaryOperations.GREATER_THAN_OR_EQUALS_ANY);
        BINARY_ANY_OPERATORS.put(Operation.LESS_THAN_OR_EQUALS, BinaryOperations.LESS_THAN_OR_EQUALS_ANY);

        BINARY_ALL_OPERATORS.put(Operation.EQUALS, BinaryOperations.EQUALS_ALL);
        BINARY_ALL_OPERATORS.put(Operation.NOT_EQUALS, BinaryOperations.NOT_EQUALS_ALL);
        BINARY_ALL_OPERATORS.put(Operation.GREATER_THAN, BinaryOperations.GREATER_THAN_ALL);
        BINARY_ALL_OPERATORS.put(Operation.LESS_THAN, BinaryOperations.LESS_THAN_ALL);
        BINARY_ALL_OPERATORS.put(Operation.GREATER_THAN_OR_EQUALS, BinaryOperations.GREATER_THAN_OR_EQUALS_ALL);
        BINARY_ALL_OPERATORS.put(Operation.LESS_THAN_OR_EQUALS, BinaryOperations.LESS_THAN_OR_EQUALS_ALL);

        UNARY_OPERATORS.put(Operation.NOT, UnaryOperations.NOT);
        UNARY_OPERATORS.put(Operation.SIZE_OF, UnaryOperations.SIZE_OF);
        UNARY_OPERATORS.put(Operation.IS_NULL, UnaryOperations.IS_NULL);
        UNARY_OPERATORS.put(Operation.IS_NOT_NULL, UnaryOperations.IS_NOT_NULL);

        N_ARY_OPERATORS.put(Operation.AND, NAryOperations.ALL_MATCH);
        N_ARY_OPERATORS.put(Operation.OR, NAryOperations.ANY_MATCH);
        N_ARY_OPERATORS.put(Operation.IF, NAryOperations.IF);
    }

    Evaluator(Expression expression) {
        type = expression.getType();
    }

    public abstract TypedObject evaluate(BulletRecord record);
}
