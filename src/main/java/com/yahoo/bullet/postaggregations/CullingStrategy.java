package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.result.Clip;

import java.util.Set;

public class CullingStrategy implements PostStrategy {
    private Set<String> transientFields;

    /**
     * Constructor for Culling strategy.
     *
     * @param culling Culling post aggregation.
     */
    public CullingStrategy(Culling culling) {
        transientFields = culling.getTransientFields();
    }

    @Override
    public Clip execute(Clip clip) {
        for (String field : transientFields) {
            clip.getRecords().forEach(record -> record.remove(field));
        }
        return clip;
    }
}
