package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.BinaryEvaluator;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * A lazy expression that takes two operands and a binary operation. These fields are required; however, an optional
 * primitive type may be specified.
 *
 * Infix and prefix binary operations are differentiated in the naming scheme.
 */
@Getter
public class BinaryExpression extends Expression {
    public static final BulletError LAZY_BINARY_REQUIRES_LEFT_AND_RIGHT = makeError("The left and right expressions must not be null.", "Please provide expressions for left and right.");
    public static final BulletError LAZY_BINARY_REQUIRES_BINARY_OPERATION = makeError("The operation must be binary.", "Please provide a binary operation for op.");
    public static final BulletError LAZY_BINARY_REQUIRES_PRIMITIVE_TYPE = makeError("The type must be primitive (if specified).", "Please provide a primitive type or no type at all.");

    @Expose
    private Expression left;
    @Expose
    private Expression right;
    @Expose
    private Operation op;

    public BinaryExpression() {
        left = null;
        right = null;
        op = null;
        type = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (left == null || right == null) {
            return Optional.of(Collections.singletonList(LAZY_BINARY_REQUIRES_LEFT_AND_RIGHT));
        }
        if (!Operation.BINARY_OPERATIONS.contains(op)) {
            return Optional.of(Collections.singletonList(LAZY_BINARY_REQUIRES_BINARY_OPERATION));
        }
        if (type != null && !Type.PRIMITIVES.contains(type)) {
            return Optional.of(Collections.singletonList(LAZY_BINARY_REQUIRES_PRIMITIVE_TYPE));
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
    public String toString() {
        return "{left: " + left + ", right: " + right + ", op: " + op + ", " + super.toString() + "}";
    }
}
