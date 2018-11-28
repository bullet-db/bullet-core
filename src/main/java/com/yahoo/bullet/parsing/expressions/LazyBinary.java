package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class LazyBinary extends LazyExpression {
    public static final BulletError LAZY_BINARY_REQUIRES_LEFT_AND_RIGHT = makeError("The left and right expressions must not be null.", "Please provide expressions for left and right.");
    public static final BulletError LAZY_BINARY_REQUIRES_BINARY_OPERATION = makeError("The operation must be binary.", "Please provide a binary operation for op.");

    @Expose
    private LazyExpression left;
    @Expose
    private LazyExpression right;
    @Expose
    private Operation op;

    public LazyBinary() {
        left = null;
        right = null;
        op = null;
        type = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        List<BulletError> errors = new ArrayList<>();
        if (left == null || right == null) {
            errors.add(LAZY_BINARY_REQUIRES_LEFT_AND_RIGHT);
        }
        if (!Operation.BINARY_OPERATIONS.contains(op)) {
            errors.add(LAZY_BINARY_REQUIRES_BINARY_OPERATION);
        }
        left.initialize().ifPresent(errors::addAll);
        right.initialize().ifPresent(errors::addAll);
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    @Override
    public String getName() {
        //return op + " (" + left.getName() + ", " + right.getName() + ")";
        return "(" + left.getName() + " " + op + " " + right.getName() + ")";
    }

    @Override
    public String toString() {
        return "{left: " + left + ", right: " + right + ", op: " + op + ", " + super.toString() + "}";
    }
}
