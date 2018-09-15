/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.typesystem.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter @AllArgsConstructor
public class Value implements Initializable {
    public enum Kind {
        @SerializedName("VALUE")
        VALUE,
        @SerializedName("FIELD")
        FIELD
    }

    public static final BulletError VALUE_OBJECT_REQUIRES_NOT_NULL_KIND_ERROR =
            makeError("The kind must not be null", "Please provide a non-null kind.");
    public static final BulletError VALUE_OBJECT_REQUIRES_NOT_NULL_VALUE_ERROR =
            makeError("The value must not be null", "Please provide a non-null value.");

    @Expose
    private Kind kind;
    @Expose
    private String value;
    @Expose
    private Type type;

    /**
     * Constructor takes a {@link Kind} and a value.
     *
     * @param kind The {@link Kind} of the Object.
     * @param value The value string of the Object.
     */
    public Value(Kind kind, String value) {
        this(kind, value, null);
    }

    @Override
    public String toString() {
        return "{kind: " + kind + ", value: " + value + ", type: " + type + "}";
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (kind == null) {
            return Optional.of(Collections.singletonList(VALUE_OBJECT_REQUIRES_NOT_NULL_KIND_ERROR));
        }
        if (value == null) {
            return Optional.of(Collections.singletonList(VALUE_OBJECT_REQUIRES_NOT_NULL_VALUE_ERROR));
        }
        return Optional.empty();
    }
}
