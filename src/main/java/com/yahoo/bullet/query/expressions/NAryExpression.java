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

@Getter
public class NAryExpression extends Expression {
    private static final long serialVersionUID = -1000391436451418013L;
    private static final String DELIMITER = ", ";

    private final List<Expression> operands;
    private final Operation op;

    public NAryExpression(List<Expression> operands, Operation op) {
        this.operands = Utilities.requireNonNull(operands);
        this.op = Objects.requireNonNull(op);
        if (!N_ARY_OPERATIONS.contains(op)) {
            throw new BulletException("N-ary expression requires an n-ary operation.", "Please specify an n-ary operation.");
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
