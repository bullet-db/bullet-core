/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.AccessLevel;
import lombok.Getter;

/**
 * Evaluators do the work of expressions.
 *
 * Each expression is built into a corresponding evaluator. Note, evaluators are constructed after a query has been
 * initialized so assume all expressions are valid.
 *
 * Evaluators are evaluated given a BulletRecord and will throw exceptions on any errors. These errors are virtually all
 * from some form of type check.
 *
 * Also, note the type cast in evaluator. For primitives, this acts as how you think it would, but for lists and maps, it
 * will cast their elements/values.
 */
public abstract class Evaluator {
    // For testing only
    @Getter(AccessLevel.PACKAGE)
    protected Type type;

    Evaluator(Expression expression) {
        type = expression.getType();
    }

    public abstract TypedObject evaluate(BulletRecord record);
}
