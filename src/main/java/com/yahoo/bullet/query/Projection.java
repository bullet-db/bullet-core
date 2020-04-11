/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

@Slf4j @Getter
public class Projection {
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
        Objects.requireNonNull(fields);
        for (Field field : fields) {
            Objects.requireNonNull(field);
        }
        this.fields = fields;
        this.type = copy ? Type.COPY : Type.NO_COPY;
    }

    @Override
    public String toString() {
        return "{fields: " + fields + ", type: " + type + " }";
    }
}
