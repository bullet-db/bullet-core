/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

@Slf4j @Getter @Setter
public class Projection implements Configurable, Initializable {
    public static final BulletError PROJECTION_FIELDS_CANNOT_CONTAIN_DELIMITERS = makeError("Projection fields cannot contain delimiters.", "Please rename your projection fields to not contain delimiters.");
    public static final String DELIMITER = ".";

    @Expose
    private List<Field> fields;

    /**
     * Default constructor. GSON recommended.
     */
    public Projection() {
        fields = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        List<BulletError> errors = new ArrayList<>();
        if (fields.stream().map(Field::getName).anyMatch(s -> s.contains(DELIMITER))) {
            errors.add(PROJECTION_FIELDS_CANNOT_CONTAIN_DELIMITERS);
        }
        fields.stream().map(Field::getValue).forEach(f -> f.initialize().ifPresent(errors::addAll));
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    @Override
    public String toString() {
        return "{fields: " + fields + "}";
    }
}
