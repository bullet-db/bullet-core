/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.NAryEvaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
@Setter
public class NAryExpression extends Expression {
    private static final BulletError N_ARY_REQUIRES_NON_NULL_LIST = makeError("The operands list must not be null.", "Please provide an operands list.");
    private static final BulletError N_ARY_REQUIRES_N_ARY_OPERATION = makeError("The operation must be n-ary.", "Please provide an n-ary operation for op.");
    private static final BulletError N_ARY_REQUIRES_PRIMITIVE_TYPE = makeError("The type must be primitive (if specified).", "Please provide a primitive type or no type at all.");
    private static final String DELIMITER = ", ";

    @Expose
    private List<Expression> operands;
    @Expose
    private Operation op;

    public NAryExpression() {
        operands = null;
        op = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (operands == null) {
            return Optional.of(Collections.singletonList(N_ARY_REQUIRES_NON_NULL_LIST));
        }
        if (!Operation.N_ARY_OPERATIONS.contains(op)) {
            return Optional.of(Collections.singletonList(N_ARY_REQUIRES_N_ARY_OPERATION));
        }
        if (type != null && !Type.PRIMITIVES.contains(type)) {
            return Optional.of(Collections.singletonList(N_ARY_REQUIRES_PRIMITIVE_TYPE));
        }
        List<BulletError> errors = new ArrayList<>();
        operands.forEach(values -> values.initialize().ifPresent(errors::addAll));
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    @Override
    public String getName() {
        return op + " (" + operands.stream().map(Expression::getName).collect(Collectors.joining(DELIMITER)) + ")";
    }

    @Override
    public Evaluator getEvaluator() {
        return new NAryEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
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
