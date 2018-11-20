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
import com.yahoo.bullet.parsing.expressions.LazyExpression;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j @Getter @Setter
public class Projection implements Configurable, Initializable {
    @Expose
    private String name;

    @Expose
    private LazyExpression value;

    /**
     * Default constructor. GSON recommended.
     */
    public Projection() {
        name = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "{name: " + getName() + ", value: " + value + "}";
    }

    private String getName() {
        return name != null ? name : value.toString();
    }
}
