package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.ValueEvaluator;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * An expression that takes a value. A primitive type must be specified since the value is always represented by a string.
 * If the type isn't specified, it's assumed to be string unless the value is null.
 */
@Getter
public class ValueExpression extends Expression {
    private static final BulletError VALUE_REQUIRES_NULL_TYPE_FOR_NULL_VALUE = makeError("The type must be null if the value is null.", "Please provide a non-null value or null type.");
    private static final BulletError VALUE_REQUIRES_PRIMITIVE_OR_NULL_TYPE = makeError("The type must be primitive or null.", "Please provide a primitive or null type.");
    private static final BulletError VALUE_REQUIRES_CASTABLE = makeError("The value must be castable to the type.", "Please provide a valid value-type pair.");

    @Expose
    private String value;

    public ValueExpression() {
        value = null;
        type = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (value == null && type == null) {
            type = Type.NULL;
        }
        if (value == null && type != Type.NULL) {
            return Optional.of(Collections.singletonList(VALUE_REQUIRES_NULL_TYPE_FOR_NULL_VALUE));
        }
        if (type == null) {
            type = Type.STRING;
        }
        if (!Type.PRIMITIVES.contains(type) && type != Type.NULL) {
            return Optional.of(Collections.singletonList(VALUE_REQUIRES_PRIMITIVE_OR_NULL_TYPE));
        }
        if (TypedObject.typeCast(type, value).getType() == Type.UNKNOWN) {
            return Optional.of(Collections.singletonList(VALUE_REQUIRES_CASTABLE));
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
