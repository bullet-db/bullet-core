package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.postaggregations.CullingStrategy;
import com.yahoo.bullet.postaggregations.PostStrategy;
import lombok.Getter;

import java.util.Set;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class Culling extends PostAggregation {
    private static final long serialVersionUID = -4606818164037391850L;

    public static final BulletError CULLING_REQUIRES_FIELDS =
            makeError("The CULLING post-aggregation requires fields.", "Please add fields.");

    private Set<String> transientFields;

    public Culling(Set<String> transientFields) {
        super(Type.CULLING);
        this.transientFields = Utilities.requireNonNullSet(transientFields);
    }

    @Override
    public PostStrategy getPostStrategy() {
        return new CullingStrategy(this);
    }
}
