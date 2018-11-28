package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Projector {
    private Map<String, Evaluator> evaluators;

    public Projector(List<Projection> projections) {
        if (projections != null && !projections.isEmpty()) {
            evaluators = new HashMap<>();
            for (Projection projection : projections) {
                evaluators.put(projection.getName(), Evaluator.build(projection.getValue()));
            }
        }
    }

    public BulletRecord project(BulletRecord record, BulletRecordProvider provider) {
        if (evaluators == null) {
            return record;
        }
        BulletRecord projected = provider.getInstance();

        return projected;
    }
}
