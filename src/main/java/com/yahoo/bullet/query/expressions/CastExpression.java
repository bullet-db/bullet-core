package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.CastEvaluator;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

@Getter @RequiredArgsConstructor
public class CastExpression extends Expression {
    private final Expression value;
    private final Type castType;

    @Override
    public String getName() {
        return "CAST (" + value.getName() + " AS " + castType + ")";
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
