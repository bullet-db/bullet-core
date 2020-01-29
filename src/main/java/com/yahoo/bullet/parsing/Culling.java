package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class Culling extends PostAggregation {
    public static final BulletError CULLING_REQUIRES_FIELDS =
            makeError("The CULLING post-aggregation requires fields.", "Please add fields.");

    @Expose
    private Set<String> transientFields;

    public Culling(Set<String> transientFields) {
        this.transientFields = transientFields;
        this.type = Type.CULLING;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = super.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (transientFields == null || transientFields.isEmpty()) {
            return Optional.of(Collections.singletonList(CULLING_REQUIRES_FIELDS));
        }
        return Optional.empty();
    }
}
