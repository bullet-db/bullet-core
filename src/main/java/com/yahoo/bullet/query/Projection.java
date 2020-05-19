/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.common.Utilities;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;

@Slf4j @Getter
public class Projection implements Serializable {
    private static final long serialVersionUID = -9194169391843941958L;

    /**
     * The type of the Projection decides how its fields are projected.
     */
    public enum Type {
        COPY,           // Projects onto a copy of the original record
        NO_COPY,        // Projects onto a new record
        PASS_THROUGH    // Passes the original record through
    }

    private final List<Field> fields;
    private final Type type;

    /**
     * Default constructor that creates a PASS_THROUGH Projection.
     */
    public Projection() {
        fields = null;
        type = Type.PASS_THROUGH;
    }

    /**
     * Constructor that creates a COPY or NO_COPY Projection.
     *
     * @param fields The list of fields to project. Must not be null or contain null fields.
     * @param copy Whether the projection should copy or not copy.
     */
    public Projection(List<Field> fields, boolean copy) {
        this.fields = Utilities.requireNonNull(fields);
        this.type = copy ? Type.COPY : Type.NO_COPY;
    }

    @Override
    public String toString() {
        return "{fields: " + fields + ", type: " + type + "}";
    }
}
