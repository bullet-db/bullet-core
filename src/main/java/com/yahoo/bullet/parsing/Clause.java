/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Slf4j @Getter @Setter
public abstract class Clause implements Configurable, Initializable {
    /** The type of the operation in the clause. */
    public enum Operation {
        @SerializedName("==")
        EQUALS,
        @SerializedName("!=")
        NOT_EQUALS,
        @SerializedName(">")
        GREATER_THAN,
        @SerializedName("<")
        LESS_THAN,
        @SerializedName(">=")
        GREATER_EQUALS,
        @SerializedName("<=")
        LESS_EQUALS,
        @SerializedName("RLIKE")
        REGEX_LIKE,
        @SerializedName("AND")
        AND,
        @SerializedName("OR")
        OR,
        @SerializedName("NOT")
        NOT;

        public static final List<String> LOGICALS = asList("AND", "OR", "NOT");
        public static final List<String> RELATIONALS = asList("==", "!=", ">=", "<=", ">", "<", "RLIKE");
    }

    @Expose
    @SerializedName(OPERATION_FIELD)
    protected Operation operation;

    public static final String OPERATION_FIELD = "operation";
    public static final BulletError OPERATION_MISSING =
        makeError("Missing operation field", "You must specify an operation field in all the filters");

    @Override
    public String toString() {
        return OPERATION_FIELD + ": " + operation;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (operation == null) {
            return Optional.of(singletonList(OPERATION_MISSING));
        }
        return Optional.empty();
    }
}

