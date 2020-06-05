/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;

/**
 * Filter consists of an evaluator built from the filter expression in the bullet query.
 *
 * Note, the filter expression does not necessarily have to have boolean type as it will be force-casted anyways.
 * Also note that if the evaluator throws an exception, the filter will not match.
 */
public class Filter {
    private Evaluator evaluator;

    public Filter(Expression filter) {
        evaluator = filter.getEvaluator();
    }

    /**
     * Checks whether the given record matches this filter.
     *
     * @param record The BulletRecord to check.
     * @return True if the record matches this filter and false otherwise.
     */
    public boolean match(BulletRecord record) {
        try {
            return (Boolean) evaluator.evaluate(record).forceCast(Type.BOOLEAN).getValue();
        } catch (Exception e) {
            return false;
        }
    }
}
