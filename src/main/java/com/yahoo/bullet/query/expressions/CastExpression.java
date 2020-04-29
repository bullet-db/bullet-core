/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.evaluators.CastEvaluator;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.Objects;

@Getter
public class CastExpression extends Expression {
    private static final long serialVersionUID = -1732511101513444236L;
    private final Expression value;
    private final Type castType;

    public CastExpression(Expression value, Type castType) {
        this.value = Objects.requireNonNull(value);
        this.castType = Objects.requireNonNull(castType);
        if (Type.isNull(castType) || Type.isUnknown(castType)) {
            throw new BulletException("Cast type cannot be null or unknown.", "Please specify a valid cast type.");
        }
    }
/*
    @Override
    public String getName() {
        return "CAST (" + value.getName() + " AS " + castType + ")";
    }
*/
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
