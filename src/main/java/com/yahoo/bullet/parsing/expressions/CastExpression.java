package com.yahoo.bullet.parsing.expressions;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.CastEvaluator;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Getter
@RequiredArgsConstructor
public class CastExpression extends Expression {
    private final Expression value;
    private final Type castType;

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }

    @Override
    public String getName() {
        return "CAST (" + value.getName() + " AS " + castType.toString() + ")";
    }

    @Override
    public Evaluator getEvaluator() {
        return new CastEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CastExpression)) {
            return false;
        }
        CastExpression other = (CastExpression) obj;
        return Objects.equals(value, other.value) && castType == other.castType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, castType);
    }

    @Override
    public String toString() {
        return "{value: " + value + ", castType: " + castType + ", " + super.toString() + "}";
    }
}
