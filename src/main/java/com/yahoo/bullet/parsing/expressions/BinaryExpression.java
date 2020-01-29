/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.BinaryEvaluator;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * An expression that takes two operands and a binary operation. These fields are required; however, an optional
 * primitive type may be specified.
 *
 * Infix and prefix binary operations are differentiated in the naming scheme.
 */
@Getter
@Setter
@RequiredArgsConstructor
public class BinaryExpression extends Expression {
    public static final BulletError BINARY_REQUIRES_LEFT_AND_RIGHT = makeError("The left and right expressions must not be null.", "Please provide expressions for left and right.");
    public static final BulletError BINARY_REQUIRES_BINARY_OPERATION = makeError("The operation must be binary.", "Please provide a binary operation for op.");

    @Expose
    private final Expression left;
    @Expose
    private final Expression right;
    @Expose
    private final Operation op;

    @Override
    public Optional<List<BulletError>> initialize() {
        if (left == null || right == null) {
            return Optional.of(Collections.singletonList(BINARY_REQUIRES_LEFT_AND_RIGHT));
        }
        if (!Operation.BINARY_OPERATIONS.contains(op)) {
            return Optional.of(Collections.singletonList(BINARY_REQUIRES_BINARY_OPERATION));
        }
        List<BulletError> errors = new ArrayList<>();
        left.initialize().ifPresent(errors::addAll);
        right.initialize().ifPresent(errors::addAll);
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    @Override
    public String getName() {
        if (op.isInfix()) {
            return "(" + left.getName() + " " + op + " " + right.getName() + ")";
        }
        return op + " (" + left.getName() + ", " + right.getName() + ")";
    }

    @Override
    public Evaluator getEvaluator() {
        return new BinaryEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof BinaryExpression)) {
            return false;
        }
        BinaryExpression other = (BinaryExpression) obj;
        return Objects.equals(left, other.left) &&
               Objects.equals(right, other.right) &&
               op == other.op;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, op);
    }

    @Override
    public String toString() {
        return "{left: " + left + ", right: " + right + ", op: " + op + ", " + super.toString() + "}";
    }
}
