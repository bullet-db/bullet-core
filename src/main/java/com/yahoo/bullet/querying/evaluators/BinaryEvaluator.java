package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.LazyBinary;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.function.BinaryOperator;

public class BinaryEvaluator extends Evaluator {
    private Evaluator left;
    private Evaluator right;
    private BinaryOperator<TypedObject> op;

    public BinaryEvaluator(LazyBinary lazyBinary) {
        super(lazyBinary);
        this.left = Evaluator.build(lazyBinary.getLeft());
        this.right = Evaluator.build(lazyBinary.getRight());
        this.op = BINARY_OPERATORS.get(lazyBinary.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        // null != TypedObject NULL
        // null => evaluation error

        TypedObject leftValue = left.evaluate(record);
        if (leftValue == null) {
            return null;
        }

        TypedObject rightValue = right.evaluate(record);
        if (rightValue == null) {
            return null;
        }

        if (type != null) {
            return op.apply(leftValue, rightValue).forceCast(type);
        }
        return op.apply(leftValue, rightValue);
    }
}
