/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EvaluatorUtils {
    public static ValueEvaluator valueEvaluator(Serializable value) {
        return new ValueEvaluator(new ValueExpression(value));
    }

    public static ListEvaluator listEvaluator(Serializable... values) {
        return new ListEvaluator(new ListExpression(Stream.of(values).map(ValueExpression::new).collect(Collectors.toCollection(ArrayList::new))));
    }

    public static FieldEvaluator fieldEvaluator(String field) {
        return new FieldEvaluator(new FieldExpression(field));
    }
}
