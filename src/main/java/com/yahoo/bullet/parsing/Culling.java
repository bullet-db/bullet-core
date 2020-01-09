package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import lombok.Getter;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@Getter
public class Culling extends PostAggregation {
    @Expose
    private Set<String> transientFields;

    public Culling(Set<String> transientFields) {
        this.transientFields = transientFields;
        this.type = Type.CULLING;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return super.initialize();
    }
}
