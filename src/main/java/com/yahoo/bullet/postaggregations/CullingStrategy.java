package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.parsing.Culling;
import com.yahoo.bullet.result.Clip;

import java.util.Set;

public class CullingStrategy implements PostStrategy {
    private Set<String> transientFields;

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
