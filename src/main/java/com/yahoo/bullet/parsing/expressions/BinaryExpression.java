/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing.expressions;

import com.yahoo.bullet.querying.evaluators.BinaryEvaluator;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

/**
 * An expression that takes two operands and a binary operation. These fields are required; however, an optional
 * primitive type may be specified.
 *
 * Infix and prefix binary operations are differentiated in the naming scheme.
 */
@Getter @RequiredArgsConstructor
public class BinaryExpression extends Expression {
    public enum Modifier {
        ANY, ALL
    }

    private final Expression left;
    private final Expression right;
    private final Operation op;
    private final Modifier modifier;

    public BinaryExpression(Expression left, Expression right, Operation op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.modifier = null;
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
               modifier == other.modifier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, op, modifier);
    }

    @Override
    public String toString() {
        return "{left: " + left + ", right: " + right + ", op: " + op + ", modifier: " + modifier + ", " + super.toString() + "}";
    }
}
