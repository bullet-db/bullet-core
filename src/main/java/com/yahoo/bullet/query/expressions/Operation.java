/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import lombok.AllArgsConstructor;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * The operations in binary, unary, and n-ary expressions.
 */
@AllArgsConstructor
public enum Operation {
    ADD("+"),
    SUB("-"),
    MUL("*"),
    DIV("/"),
    EQUALS("="),
    EQUALS_ANY("= ANY"),
    EQUALS_ALL("= ALL"),
    NOT_EQUALS("!="),
    NOT_EQUALS_ANY("!= ANY"),
    NOT_EQUALS_ALL("!= ALL"),
    GREATER_THAN(">"),
    GREATER_THAN_ANY("> ANY"),
    GREATER_THAN_ALL("> ALL"),
    LESS_THAN("<"),
    LESS_THAN_ANY("< ANY"),
    LESS_THAN_ALL("< ALL"),
    GREATER_THAN_OR_EQUALS(">="),
    GREATER_THAN_OR_EQUALS_ANY(">= ANY"),
    GREATER_THAN_OR_EQUALS_ALL(">= ALL"),
    LESS_THAN_OR_EQUALS("<="),
    LESS_THAN_OR_EQUALS_ANY("<= ANY"),
    LESS_THAN_OR_EQUALS_ALL("<= ALL"),
    REGEX_LIKE("RLIKE"),
    REGEX_LIKE_ANY("RLIKE ANY"),
    SIZE_IS("SIZEIS"),
    CONTAINS_KEY("CONTAINSKEY"),
    CONTAINS_VALUE("CONTAINSVALUE"),
    IN("IN"),
    NOT_IN("NOT IN"),
    AND("AND"),
    OR("OR"),
    XOR("XOR"),
    FILTER("FILTER"),
    NOT("NOT"),
    SIZE_OF("SIZEOF"),
    IS_NULL("IS NULL"),
    IS_NOT_NULL("IS NOT NULL"),
    IF("IF");

    public static final Set<Operation> BINARY_OPERATIONS =
            new HashSet<>(asList(ADD, SUB, MUL, DIV,
                                 EQUALS, EQUALS_ANY, EQUALS_ALL, NOT_EQUALS, NOT_EQUALS_ANY, NOT_EQUALS_ALL,
                                 GREATER_THAN, GREATER_THAN_ANY, GREATER_THAN_ALL, LESS_THAN, LESS_THAN_ANY, LESS_THAN_ALL,
                                 GREATER_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS_ANY, GREATER_THAN_OR_EQUALS_ALL,
                                 LESS_THAN_OR_EQUALS, LESS_THAN_OR_EQUALS_ANY, LESS_THAN_OR_EQUALS_ALL, REGEX_LIKE, REGEX_LIKE_ANY,
                                 SIZE_IS, CONTAINS_KEY, CONTAINS_VALUE, IN, NOT_IN, AND, OR, XOR, FILTER));
    public static final Set<Operation> UNARY_OPERATIONS = new HashSet<>(asList(NOT, SIZE_OF, IS_NULL, IS_NOT_NULL));
    public static final Set<Operation> N_ARY_OPERATIONS = new HashSet<>(asList(AND, OR, IF));

    private String name;

    @Override
    public String toString() {
        return name;
    }
}
