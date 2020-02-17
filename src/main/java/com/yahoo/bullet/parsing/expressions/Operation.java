/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * The type of the operation in binary/unary expressions.
 */
@Getter
@AllArgsConstructor
public enum Operation {
    @SerializedName("+")
    ADD("+", true),
    @SerializedName("-")
    SUB("-", true),
    @SerializedName("*")
    MUL("*", true),
    @SerializedName("/")
    DIV("/", true),
    @SerializedName("=")
    EQUALS("=", true),
    @SerializedName("!=")
    NOT_EQUALS("!=", true),
    @SerializedName(">")
    GREATER_THAN(">", true),
    @SerializedName("<")
    LESS_THAN("<", true),
    @SerializedName(">=")
    GREATER_THAN_OR_EQUALS(">=", true),
    @SerializedName("<=")
    LESS_THAN_OR_EQUALS("<=", true),
    @SerializedName(value = "~=", alternate = { "RLIKE" })
    REGEX_LIKE("RLIKE", false),
    @SerializedName("SIZEIS")
    SIZE_IS("SIZEIS", false),
    @SerializedName("CONTAINSKEY")
    CONTAINS_KEY("CONTAINSKEY", false),
    @SerializedName("CONTAINSVALUE")
    CONTAINS_VALUE("CONTAINSVALUE", false),
    @SerializedName("IN")
    IN("IN", true),
    @SerializedName(value = "AND", alternate = { "&&" })
    AND("AND", true),
    @SerializedName(value = "OR", alternate = { "||" })
    OR("OR", true),
    @SerializedName(value = "XOR", alternate = { "^" })
    XOR("XOR", true),
    @SerializedName("FILTER")
    FILTER("FILTER", false),
    @SerializedName(value = "NOT", alternate = { "~" })
    NOT("NOT", false),
    @SerializedName("SIZEOF")
    SIZE_OF("SIZEOF", false),
    @SerializedName("ISNULL")
    IS_NULL("ISNULL", false),
    @SerializedName("ISNOTNULL")
    IS_NOT_NULL("ISNOTNULL", false),
    @SerializedName(value = "IF", alternate = { "?" })
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
