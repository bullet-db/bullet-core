package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.UnaryExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.function.UnaryOperator;

/**
 * Evaluator that evaluates the operand before applying a unary operator. Casts the result.
 */
public class UnaryEvaluator extends Evaluator {
    private Evaluator operand;
    private UnaryOperator<TypedObject> op;

    public UnaryEvaluator(UnaryExpression unaryExpression) {
        super(unaryExpression);
        this.operand = Evaluator.build(unaryExpression.getOperand());
        this.op = UNARY_OPERATORS.get(unaryExpression.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        TypedObject value = operand.evaluate(record);
        TypedObject result = op.apply(value);
        return cast(result);
    }
}
