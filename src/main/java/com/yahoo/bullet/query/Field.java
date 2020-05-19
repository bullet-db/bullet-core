/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.query.expressions.Expression;
import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

@Getter
public class Field implements Serializable {
    private static final long serialVersionUID = -2084429671585261042L;

    private String name;
    private Expression value;

    public Field(String name, Expression value) {
        this.name = Objects.requireNonNull(name);
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Field)) {
            return false;
        }
        Field other = (Field) obj;
        return Objects.equals(name, other.name) && Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "{name: " + name + ", value: " + value + "}";
    }
}
