/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class ComputationStrategy implements PostStrategy {
    private Computation computation;
    private Map<String, Evaluator> evaluators;

    public ComputationStrategy(Computation computation) {
        this.computation = computation;
        this.evaluators = new LinkedHashMap<>();
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = computation.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        computation.getProjection().getFields().forEach(field -> evaluators.put(field.getName(), Evaluator.build(field.getValue())));
        return Optional.empty();
    }

    @Override
    public Clip execute(Clip clip) {
        clip.getRecords().forEach(r -> {
            evaluators.forEach((name, evaluator) -> {
                try {
                    TypedObject value = evaluator.evaluate(r);
                    if (value != null && value.getValue() != null) {
                        r.forceSet(name, value.getValue());
                    }
                } catch (Exception ignored) {
                }
            });
        });
        return clip;
    }

    @Override
    public Set<String> getRequiredFields() {
        return Collections.emptySet();
    }
}
