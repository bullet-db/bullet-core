/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.parsing.Having;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.Type;

public class HavingStrategy implements PostStrategy {
    private Evaluator evaluator;

    /**
     *
     * @param having
     */
    public HavingStrategy(Having having) {
        evaluator = Evaluator.build(having.getExpression());
    }

    @Override
    public Clip execute(Clip clip) {
        /*
        List<BulletRecord> records = clip.getRecords();
        records = records.stream().filter(r -> {
                try {
                    return evaluator.evaluate(r).forceCast(Type.BOOLEAN).getBoolean();
                } catch (Exception ignored) {
                    return false;
                }
            }).collect(Collectors.toList());
        return Clip.of(records).add(clip.getMeta());
        */
        clip.getRecords().removeIf(record -> {
            try {
                return !evaluator.evaluate(record).forceCast(Type.BOOLEAN).getBoolean();
            } catch (Exception ignored) {
                return true;
            }
        });
        return clip;
    }
}
