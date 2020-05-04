/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * The type of the operation in binary/unary expressions.
 */
@Getter @AllArgsConstructor
public enum Operation {
    ADD("+", true),
    SUB("-", true),
    MUL("*", true),
    DIV("/", true),
    EQUALS("=", true),
    NOT_EQUALS("!=", true),
    GREATER_THAN(">", true),
    LESS_THAN("<", true),
    GREATER_THAN_OR_EQUALS(">=", true),
    LESS_THAN_OR_EQUALS("<=", true),
    REGEX_LIKE("RLIKE", false),
    SIZE_IS("SIZEIS", false),
    CONTAINS_KEY("CONTAINSKEY", false),
    CONTAINS_VALUE("CONTAINSVALUE", false),
    IN("IN", true),
    AND("AND", true),
    OR("OR", true),
    XOR("XOR", true),
    FILTER("FILTER", false),
    NOT("NOT", false),
    SIZE_OF("SIZEOF", false),
    IS_NULL("ISNULL", false),
    IS_NOT_NULL("ISNOTNULL", false),
    IF("IF", false);

    public static final Set<Operation> BINARY_OPERATIONS =
            new HashSet<>(asList(ADD, SUB, MUL, DIV,
                                 EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, GREATER_THAN_OR_EQUALS, LESS_THAN_OR_EQUALS,
                                 REGEX_LIKE, SIZE_IS, CONTAINS_KEY, CONTAINS_VALUE, IN,
                                 AND, OR, XOR,
                                 FILTER));
    public static final Set<Operation> UNARY_OPERATIONS = new HashSet<>(asList(NOT, SIZE_OF, IS_NULL, IS_NOT_NULL));
    public static final Set<Operation> N_ARY_OPERATIONS = new HashSet<>(asList(AND, OR, IF));
    public static final Set<Operation> LOGICALS = new HashSet<>(asList(AND, OR, XOR, NOT));
    public static final Set<Operation> RELATIONALS =
            new HashSet<>(asList(EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, GREATER_THAN_OR_EQUALS, LESS_THAN_OR_EQUALS,
                                 REGEX_LIKE, SIZE_IS, CONTAINS_KEY, CONTAINS_VALUE));

    private String name;
    private boolean infix;

    @Override
    public String toString() {
        return name;
    }
}
