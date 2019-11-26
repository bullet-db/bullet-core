package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Having;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.Type;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HavingStrategy implements PostStrategy {
    private Having having;
    private Evaluator evaluator;

    public HavingStrategy(Having having) {
        this.having = having;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = having.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        evaluator = Evaluator.build(having.getExpression());
        return Optional.empty();
    }

    @Override
    public Clip execute(Clip clip) {
        List<BulletRecord> records = clip.getRecords();
        // records is an ArrayList ...
        records = records.stream().filter(r -> {
            try {
                return (Boolean) evaluator.evaluate(r).forceCast(Type.BOOLEAN).getValue();
            } catch (Exception ignored) {
                return false;
            }
        }).collect(Collectors.toList());
        return Clip.of(records).add(clip.getMeta());
    }

    @Override
    public Set<String> getRequiredFields() {
        return Collections.emptySet();
    }
}
