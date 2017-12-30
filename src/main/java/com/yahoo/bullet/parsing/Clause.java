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
import com.yahoo.bullet.querying.FilterOperations.FilterType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.parsing.ParsingError.makeError;
import static java.util.Collections.singletonList;

@Slf4j @Getter @Setter
public abstract class Clause implements Configurable, Initializable {
    @Expose
    @SerializedName(OPERATION_FIELD)
    protected FilterType operation;

    public static final String OPERATION_FIELD = "operation";
    public static final ParsingError OPERATION_MISSING =
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

