/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.NAryEvaluator;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

import static com.yahoo.bullet.query.expressions.Operation.N_ARY_OPERATIONS;

/**
 * An expression that requires a list of operands and an n-ary operation.
 */
@Getter
public class NAryExpression extends Expression {
    private static final long serialVersionUID = -1000391436451418013L;
    private static final BulletException N_ARY_EXPRESSION_REQUIRES_N_ARY_OPERATION =
            new BulletException("N-ary expression requires an n-ary operation.", "Please specify an n-ary operation.");
    private static final String DELIMITER = ", ";

    private final List<Expression> operands;
    private final Operation op;

    /**
     * Constructor that creates an n-ary expression.
     *
     * @param operands The non-null list of operands.
     * @param op The non-null n-ary operation.
     */
    public NAryExpression(List<Expression> operands, Operation op) {
        this.operands = Utilities.requireNonNull(operands);
        this.op = Objects.requireNonNull(op);
        if (!N_ARY_OPERATIONS.contains(op)) {
            throw N_ARY_EXPRESSION_REQUIRES_N_ARY_OPERATION;
        }
    }

    @Override
    public Evaluator getEvaluator() {
        return new NAryEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof NAryExpression)) {
            return false;
        }
        NAryExpression other = (NAryExpression) obj;
        return Objects.equals(operands, other.operands) && op == other.op && type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operands, op, type);
    }

    @Override
    public String toString() {
        return "{operands: " + operands + ", op: " + op + ", " + super.toString() + "}";
    }
}
