/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.UnaryEvaluator;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * An expression that takes an operand and a unary operation. These fields are required; however, an optional
 * primitive type may be specified.
 */
@Getter
@Setter
@RequiredArgsConstructor
public class UnaryExpression extends Expression {
    public static final BulletError UNARY_REQUIRES_NON_NULL_OPERAND = makeError("The operand must not be null.", "Please provide an expression for operand.");
    public static final BulletError UNARY_REQUIRES_UNARY_OPERATION = makeError("The operation must be unary.", "Please provide a unary operation for op.");

    @Expose
    private final Expression operand;
    @Expose
    private final Operation op;

    @Override
    public Optional<List<BulletError>> initialize() {
        if (operand == null) {
            return Optional.of(Collections.singletonList(UNARY_REQUIRES_NON_NULL_OPERAND));
        }
        if (!Operation.UNARY_OPERATIONS.contains(op)) {
            return Optional.of(Collections.singletonList(UNARY_REQUIRES_UNARY_OPERATION));
        }
        return operand.initialize();
    }

    @Override
    public String getName() {
        return op + " (" + operand.getName() + ")";
    }

    @Override
    public Evaluator getEvaluator() {
        return new UnaryEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof UnaryExpression)) {
            return false;
        }
        UnaryExpression other = (UnaryExpression) obj;
        return Objects.equals(operand, other.operand) && op == other.op && type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operand, op, type);
    }

    @Override
    public String toString() {
        return "{operand: " + operand + ", op: " + op + ", " + super.toString() + "}";
    }
}
