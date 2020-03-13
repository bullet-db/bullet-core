/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing.expressions;

import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.ValueEvaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

/**
 * An expression that takes a value. A primitive type must be specified since the value is always represented by a string.
 * If the type isn't specified, it's assumed to be string unless the value is null.
 */
@Getter @Setter @NoArgsConstructor
public class ValueExpression extends Expression {
    private Object value;

    public ValueExpression(Object value) {
        this.value = value;
        this.type = Type.getType(value);
    }

    @Override
    public String getName() {
        if (type == Type.STRING) {
            return "'" + value.toString() + "'";
        }
        return value.toString();
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

    @Override
    public String toString() {
        return "{value: " + getName() + ", " + super.toString() + "}";
    }
}
