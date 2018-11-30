package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.expressions.LazyExpression;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;

/**
 * Filter consists of an evaluator built from the filter lazy expression in the bullet query. A null filter will always
 * return a match.
 *
 * Note, the filter's lazy expression does not necessarily have to have boolean type as it will be force-casted anyways.
 * Also note that if the evaluator throws an exception, the filter will not match.
 */
public class Filter {
    private Evaluator evaluator;

    public Filter(LazyExpression filter) {
        evaluator = Evaluator.build(filter);
    }

    /**
     * Checks whether the given record matches this filter.
     *
     * @param record The BulletRecord to check.
     * @return True if the record matches this filter and false otherwise.
     */
    public boolean match(BulletRecord record) {
        if (evaluator == null) {
            return true;
        }
        try {
            return (Boolean) evaluator.evaluate(record).forceCast(Type.BOOLEAN).getValue();
        } catch (Exception e) {
            return false;
        }
    }
}
