package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

public class LazyUnary extends LazyExpression {
    public static final BulletError LAZY_UNARY_REQUIRES_NON_NULL_VALUE = makeError("The value must not be null.", "Please provide an expression for value.");
    public static final BulletError LAZY_UNARY_REQUIRES_UNARY_OPERATION = makeError("The operation must be unary.", "Please provide a unary operation for op.");

    @Expose
    private LazyExpression value;
    @Expose
    private Operation op;

    @Override
    public Optional<List<BulletError>> initialize() {
        List<BulletError> errors = new ArrayList<>();
        if (value == null) {
            errors.add(LAZY_UNARY_REQUIRES_NON_NULL_VALUE);
        }
        if (!Operation.UNARY_OPERATIONS.contains(op)) {
            errors.add(LAZY_UNARY_REQUIRES_UNARY_OPERATION);
        }
        if (value != null) {
            value.initialize().ifPresent(errors::addAll);
        }
        return Optional.of(errors);
    }

    @Override
    public String getName() {
        return op + " (" + value.getName() + ")";
    }

    @Override
    public String toString() {
        return "{value: " + value + ", op: " + op + ", " + super.toString() + "}";
    }
}
