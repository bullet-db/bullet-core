/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.ValueEvaluator;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * An expression that takes a value. A primitive type must be specified since the value is always represented by a string.
 * If the type isn't specified, it's assumed to be string unless the value is null.
 */
@Getter
@Setter
public class ValueExpression extends Expression {
    private static final BulletError VALUE_REQUIRES_NULL_TYPE_FOR_NULL_VALUE = makeError("The type must be null if the value is null.", "Please provide a non-null value or null type.");
    private static final BulletError VALUE_REQUIRES_PRIMITIVE_OR_NULL_TYPE = makeError("The type must be primitive or null.", "Please provide a primitive or null type.");
    private static final BulletError VALUE_REQUIRES_CASTABLE = makeError("The value must be castable to the type.", "Please provide a valid value-type pair.");

    @Expose
    private Object value;

    public ValueExpression() {
        value = null;
        type = null;
    }

    public ValueExpression(Object value) {
        this.value = value;
        this.type = Type.getType(value);
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (value == null && type != Type.NULL) {
            return Optional.of(Collections.singletonList(VALUE_REQUIRES_NULL_TYPE_FOR_NULL_VALUE));
        }
        if (!Type.PRIMITIVES.contains(type) && type != Type.NULL) {
            return Optional.of(Collections.singletonList(VALUE_REQUIRES_PRIMITIVE_OR_NULL_TYPE));
        }
        if (TypedObject.typeCastFromObject(type, value).getType() == Type.UNKNOWN) {
            return Optional.of(Collections.singletonList(VALUE_REQUIRES_CASTABLE));
        }
        return Optional.empty();
    }

    @Override
    public String getName() {
        if (type == Type.STRING) {
            return '"' + value.toString() + '"';
        }
        return value.toString();
    }

    @Override
    public Evaluator getEvaluator() {
        return new ValueEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ValueExpression)) {
            return false;
        }
        ValueExpression other = (ValueExpression) obj;
        return Objects.equals(value, other.value) && type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type);
    }

    @Override
    public String toString() {
        return "{value: " + getName() + "}";
    }
}
