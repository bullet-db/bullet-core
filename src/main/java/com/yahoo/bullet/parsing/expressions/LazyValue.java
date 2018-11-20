package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

public class LazyValue extends LazyExpression {
    private static final BulletError LAZY_VALUE_REQUIRES_NON_NULL_VALUE = makeError("The value must not be null.", "Please provide a non-null value.");

    @Expose
    private Object value;

    @Override
    public Optional<List<BulletError>> initialize() {
        if (value == null) {
            return Optional.of(Collections.singletonList(LAZY_VALUE_REQUIRES_NON_NULL_VALUE));
        }

        // TODO primitive type check


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
    public String toString() {
        return "{value: " + getName() + "}";
    }
}
