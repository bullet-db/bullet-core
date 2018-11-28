package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class LazyUnary extends LazyExpression {
    public static final BulletError LAZY_UNARY_REQUIRES_NON_NULL_OPERAND = makeError("The operand must not be null.", "Please provide an expression for operand.");
    public static final BulletError LAZY_UNARY_REQUIRES_UNARY_OPERATION = makeError("The operation must be unary.", "Please provide a unary operation for op.");

    @Expose
    private LazyExpression operand;
    @Expose
    private Operation op;

    public LazyUnary() {
        operand = null;
        op = null;
        type = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        List<BulletError> errors = new ArrayList<>();
        if (operand == null) {
            errors.add(LAZY_UNARY_REQUIRES_NON_NULL_OPERAND);
        }
        if (!Operation.UNARY_OPERATIONS.contains(op)) {
            errors.add(LAZY_UNARY_REQUIRES_UNARY_OPERATION);
        }
        if (operand != null) {
            operand.initialize().ifPresent(errors::addAll);
        }
        return Optional.of(errors);
    }

    @Override
    public String getName() {
        return op + " (" + operand.getName() + ")";
    }

    @Override
    public String toString() {
        return "{operand: " + operand + ", op: " + op + ", " + super.toString() + "}";
    }
}
