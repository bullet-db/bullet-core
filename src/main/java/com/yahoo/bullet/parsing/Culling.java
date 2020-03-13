package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletError;
import lombok.Getter;

import java.util.Set;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class Culling extends PostAggregation {
    public static final BulletError CULLING_REQUIRES_FIELDS =
            makeError("The CULLING post-aggregation requires fields.", "Please add fields.");

    private Set<String> transientFields;

    public Culling(Set<String> transientFields) {
        this.transientFields = transientFields;
        this.type = Type.CULLING;
    }
}
