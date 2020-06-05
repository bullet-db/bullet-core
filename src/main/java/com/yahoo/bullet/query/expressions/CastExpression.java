/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.evaluators.CastEvaluator;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.Objects;

/**
 * An expression that requires a value and a cast type.
 */
@Getter
public class CastExpression extends Expression {
    private static final long serialVersionUID = -1732511101513444236L;
    private static final BulletException CAST_EXPRESSION_REQUIRES_VALID_TYPE =
            new BulletException("Cast type cannot be null or unknown.", "Please specify a valid cast type.");

    private final Expression value;
    private final Type castType;

    /**
     * Constructor that creates a cast expression.
     *
     * @param value The non-null value to cast.
     * @param castType The non-null cast type to cast the value to.
     */
    public CastExpression(Expression value, Type castType) {
        this.value = Objects.requireNonNull(value);
        this.castType = Objects.requireNonNull(castType);
        if (Type.isNull(castType) || Type.isUnknown(castType)) {
            throw CAST_EXPRESSION_REQUIRES_VALID_TYPE;
        }
    }

    @Override
    public Evaluator getEvaluator() {
        return new CastEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof CastExpression)) {
            return false;
        }
        CastExpression other = (CastExpression) obj;
        return Objects.equals(value, other.value) && castType == other.castType && type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, castType, type);
    }

    @Override
    public String toString() {
        return "{value: " + value + ", castType: " + castType + ", " + super.toString() + "}";
    }
}
