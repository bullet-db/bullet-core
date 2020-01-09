package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.result.Clip;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@AllArgsConstructor
public class TransientStrategy implements PostStrategy {
    private Set<String> transientFields;

    @Override
    public Optional<List<BulletError>> initialize() {
        if (transientFields == null || transientFields.isEmpty()) {

        }
        return Optional.empty();
    }

    @Override
    public Clip execute(Clip clip) {
        for (String field : transientFields) {
            clip.getRecords().forEach(record -> record.remove(field));
        }
        return clip;
    }
}
