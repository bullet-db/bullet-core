/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j @Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class Projection {
    private List<Field> fields;
    private boolean copy;

    public Projection(List<Field> fields) {
        this.fields = fields;
        this.copy = false;
    }

    @Override
    public String toString() {
        return "{fields: " + fields + ", copy: " + copy + " }";
    }
}
