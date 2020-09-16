/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;

/**
 * Evaluators are built from expressions. They are evaluated given a {@link BulletRecord} and will throw exceptions on
 * any errors which are most likely to be the result of missing fields or incorrect types.
 */
public abstract class Evaluator implements Serializable {
    private static final long serialVersionUID = 8998958368200061680L;

    /**
     * Evaluates this evaluator on the given {@link BulletRecord}.
     *
     * @param record The Bullet record to evaluate this evaluator on.
     * @return The result of this evaluator on the given Bullet record.
     */
    public abstract TypedObject evaluate(BulletRecord record);
}
