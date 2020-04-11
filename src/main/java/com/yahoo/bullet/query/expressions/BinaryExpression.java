/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.BinaryEvaluator;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import lombok.Getter;

import java.util.Objects;

/**
 * An expression that takes two operands and a binary operation. These fields are required; however, an optional
 * primitive type may be specified.
 *
 * Infix and prefix binary operations are differentiated in the naming scheme.
 */
@Getter
public class BinaryExpression extends Expression {
    private static final long serialVersionUID = -7911485746578844403L;

    public enum Modifier {
        ANY, ALL, NONE
    }

    private final Expression left;
    private final Expression right;
    private final Operation op;
    private final Modifier modifier;

    public BinaryExpression(Expression left, Expression right, Operation op) {
        this(left, right, op, Modifier.NONE);
    }

    public BinaryExpression(Expression left, Expression right, Operation op, Modifier modifier) {
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
        this.op = Objects.requireNonNull(op);
        this.modifier = Objects.requireNonNull(modifier);
    }

    @Override
    public String getName() {
        if (op.isInfix()) {
            if (modifier != null) {
                return "(" + left.getName() + " " + op + " " + modifier + " " + right.getName() + ")";
            }
            return "(" + left.getName() + " " + op + " " + right.getName() + ")";
        }
        return op + "(" + left.getName() + ", " + right.getName() + ")";
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
               op == other.op &&
               modifier == other.modifier &&
               type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, op, modifier, type);
    }

    @Override
    public String toString() {
        return "{left: " + left + ", right: " + right + ", op: " + op + ", modifier: " + modifier + ", " + super.toString() + "}";
    }
}
