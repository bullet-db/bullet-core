/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j @Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class Projection implements Configurable, Initializable {
    @Expose
    private List<Field> fields;
    @Expose
    private boolean copy;

    public Projection(List<Field> fields) {
        this.fields = fields;
        this.copy = false;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        List<BulletError> errors = new ArrayList<>();
        if (fields != null) {
            if (fields.isEmpty()) {
                // error
            }
            fields.stream().map(Field::getValue).forEach(f -> f.initialize().ifPresent(errors::addAll));
        }
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    @Override
    public String toString() {
        return "{fields: " + fields + ", copy: " + copy + " }";
    }
}
