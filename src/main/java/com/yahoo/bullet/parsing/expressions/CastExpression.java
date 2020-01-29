package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.CastEvaluator;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
@RequiredArgsConstructor
public class CastExpression extends Expression {
    private static final BulletError CAST_REQUIRES_NON_NULL_VALUE = makeError("The value must not be null.", "Please provide a non-null value.");
    private static final BulletError CAST_REQUIRES_PRIMITIVE_CAST_TYPE = makeError("The cast type must be primitive.", "Please provide a primitive cast type.");

    @Expose
    private final Expression value;
    @Expose
    private final Type castType;

    @Override
    public Optional<List<BulletError>> initialize() {
        if (value == null) {
            return Optional.of(Collections.singletonList(CAST_REQUIRES_NON_NULL_VALUE));
        }
        if (!Type.PRIMITIVES.contains(castType)) {
            return Optional.of(Collections.singletonList(CAST_REQUIRES_PRIMITIVE_CAST_TYPE));
        }
        return Optional.empty();
    }

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
