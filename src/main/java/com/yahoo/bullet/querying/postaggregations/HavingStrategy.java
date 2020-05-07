/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.postaggregations;

import com.yahoo.bullet.query.postaggregations.Having;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.Type;

public class HavingStrategy implements PostStrategy {
    private final Evaluator evaluator;

    /**
     * Constructor for Having strategy.
     *
     * @param having Having post aggregation.
     */
    public HavingStrategy(Having having) {
        evaluator = having.getExpression().getEvaluator();
    }

    @Override
    public Clip execute(Clip clip) {
        clip.getRecords().removeIf(record -> {
            try {
                return !((Boolean) evaluator.evaluate(record).forceCast(Type.BOOLEAN).getValue());
            } catch (Exception ignored) {
                return true;
            }
        });
        return clip;
    }
}
