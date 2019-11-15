package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.ValueEvaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * An expression that takes a value. A primitive type must be specified since the value is always represented by a string.
 * If the type isn't specified, it's assumed to be string.
 */
@Getter
public class ValueExpression extends Expression {
    private static final BulletError VALUE_REQUIRES_NON_NULL_VALUE = makeError("The value must not be null.", "Please provide a non-null value.");
    private static final BulletError VALUE_REQUIRES_PRIMITIVE_TYPE = makeError("The type must be primitive.", "Please provide a primitive type.");

    @Expose
    private String value;

    public ValueExpression() {
        value = null;
        type = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (value == null) {
            return Optional.of(Collections.singletonList(VALUE_REQUIRES_NON_NULL_VALUE));
        }
        if (!Type.PRIMITIVES.contains(type)) {
            return Optional.of(Collections.singletonList(VALUE_REQUIRES_PRIMITIVE_TYPE));
        }
        return Optional.empty();
    }

    @Override
    public String getName() {
        if (type == Type.STRING) {
            return '"' + value + '"';
        }
        return value;
    }

    @Override
    public Evaluator getEvaluator() {
        return new ValueEvaluator(this);
    }

    @Override
    public String toString() {
        return "{value: " + getName() + "}";
    }
}
