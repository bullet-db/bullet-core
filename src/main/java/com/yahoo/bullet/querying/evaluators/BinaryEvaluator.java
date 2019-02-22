package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.function.BinaryOperator;

/**
 * Evaluator that evaluates the left and right before applying a binary operator. Casts the result.
 */
public class BinaryEvaluator extends Evaluator {
    private Evaluator left;
    private Evaluator right;
    private BinaryOperator<TypedObject> op;

    public BinaryEvaluator(BinaryExpression binaryExpression) {
        super(binaryExpression);
        this.left = Evaluator.build(binaryExpression.getLeft());
        this.right = Evaluator.build(binaryExpression.getRight());
        this.op = BINARY_OPERATORS.get(binaryExpression.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        TypedObject result = op.apply(leftValue, rightValue);
        return cast(result);
    }
}
