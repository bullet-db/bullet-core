package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.NAryExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.List;
import java.util.stream.Collectors;

public class NAryEvaluator extends Evaluator {
    private List<Evaluator> operands;
    private NAryOperator op;

    public NAryEvaluator(NAryExpression nAryExpression) {
        super(nAryExpression);
        this.operands = nAryExpression.getOperands().stream().map(Evaluator::build).collect(Collectors.toList());
        this.op = N_ARY_OPERATORS.get(nAryExpression.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return cast(op.apply(operands, record));
    }
}
