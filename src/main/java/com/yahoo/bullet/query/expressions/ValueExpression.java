/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.ValueEvaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

/**
 * An expression that takes a value. A primitive or null must be specified.
 */
@Getter
public class ValueExpression extends Expression {
    private static final long serialVersionUID = -6979831483897873098L;

    private final Serializable value;

    public ValueExpression(Serializable value) {
        this.value = value;
        this.type = Type.getType(value);
        if (!Type.isPrimitive(type) && !Type.isNull(type)) {
            throw new BulletException("Value must be primitive or null.", "Please specify a valid value.");
        }
    }

    @Override
    public Evaluator getEvaluator() {
        return new ValueEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
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

    private String toFormattedString() {
        if (type == Type.STRING) {
            return "'" + value.toString() + "'";
        } else if (type == Type.NULL) {
            return "null";
        }
        return value.toString();
    }

    @Override
    public String toString() {
        return "{value: " + toFormattedString() + ", " + super.toString() + "}";
    }
}
