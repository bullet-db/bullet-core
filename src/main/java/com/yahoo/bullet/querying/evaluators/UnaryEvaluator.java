package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.LazyUnary;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.function.UnaryOperator;

public class UnaryEvaluator extends Evaluator {
    private Evaluator operand;
    private UnaryOperator<TypedObject> op;

    public UnaryEvaluator(LazyUnary lazyUnary) {
        super(lazyUnary);
        this.operand = Evaluator.build(lazyUnary.getOperand());
        this.op = UNARY_OPERATORS.get(lazyUnary.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        TypedObject value = operand.evaluate(record);
        TypedObject result = op.apply(value);
        return cast(result);
    }
}
