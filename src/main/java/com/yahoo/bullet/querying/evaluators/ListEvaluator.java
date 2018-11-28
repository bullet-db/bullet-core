package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.LazyList;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.List;
import java.util.stream.Collectors;

public class ListEvaluator extends Evaluator {
    private List<Evaluator> evaluators;

    public ListEvaluator(LazyList lazyList) {
        super(lazyList);
        this.evaluators = lazyList.getValues().stream().map(Evaluator::build).collect(Collectors.toList());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        // type?
        // subtype?
        return new TypedObject(evaluators.stream().map(e -> e.evaluate(record).forceCast(type).getValue()).collect(Collectors.toList()));
    }
}
