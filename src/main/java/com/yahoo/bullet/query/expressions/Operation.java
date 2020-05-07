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
    EQUALS_ANY("= ANY", true),
    EQUALS_ALL("= ALL", true),
    NOT_EQUALS("!=", true),
    NOT_EQUALS_ANY("!= ANY", true),
    NOT_EQUALS_ALL("!= ALL", true),
    GREATER_THAN(">", true),
    GREATER_THAN_ANY("> ANY", true),
    GREATER_THAN_ALL("> ALL", true),
    LESS_THAN("<", true),
    LESS_THAN_ANY("< ANY", true),
    LESS_THAN_ALL("< ALL", true),
    GREATER_THAN_OR_EQUALS(">=", true),
    GREATER_THAN_OR_EQUALS_ANY(">= ANY", true),
    GREATER_THAN_OR_EQUALS_ALL(">= ALL", true),
    LESS_THAN_OR_EQUALS("<=", true),
    LESS_THAN_OR_EQUALS_ANY("<= ANY", true),
    LESS_THAN_OR_EQUALS_ALL("<= ALL", true),
    REGEX_LIKE("RLIKE", false),
    REGEX_LIKE_ANY("RLIKE_ANY", false),
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
                                 EQUALS, EQUALS_ANY, EQUALS_ALL, NOT_EQUALS, NOT_EQUALS_ANY, NOT_EQUALS_ALL,
                                 GREATER_THAN, GREATER_THAN_ANY, GREATER_THAN_ALL, LESS_THAN, LESS_THAN_ANY, LESS_THAN_ALL,
                                 GREATER_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS_ANY, GREATER_THAN_OR_EQUALS_ALL,
                                 LESS_THAN_OR_EQUALS, LESS_THAN_OR_EQUALS_ANY, LESS_THAN_OR_EQUALS_ALL, REGEX_LIKE, REGEX_LIKE_ANY,
                                 SIZE_IS, CONTAINS_KEY, CONTAINS_VALUE, IN, AND, OR, XOR, FILTER));
    public static final Set<Operation> UNARY_OPERATIONS = new HashSet<>(asList(NOT, SIZE_OF, IS_NULL, IS_NOT_NULL));
    public static final Set<Operation> N_ARY_OPERATIONS = new HashSet<>(asList(AND, OR, IF));

    private String name;
    private boolean infix;

    @Override
    public String toString() {
        return name;
    }
}
