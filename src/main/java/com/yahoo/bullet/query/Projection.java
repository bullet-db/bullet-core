/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j @Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class Projection {
    /**
     * Represents the type of the Projection.
     */
    public enum Type {
        // Projects onto a copy of the record
        COPY,
        // Projects to an empty record
        NO_COPY,
        // Does nothing
        PASS_THROUGH
    }

    private List<Field> fields;
    private Type type;

    public Projection(List<Field> fields) {
        this.fields = fields;
        this.type = Type.NO_COPY;
    }

    @Override
    public String toString() {
        return "{fields: " + fields + ", type: " + type + " }";
    }
}
