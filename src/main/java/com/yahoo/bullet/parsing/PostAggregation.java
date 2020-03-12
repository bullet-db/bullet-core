/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Initializable;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;
import static java.util.Collections.singletonList;

@Getter @Setter
public abstract class PostAggregation implements Initializable {
    public static final String ORDER_BY_SERIALIZED_NAME = "ORDER BY";
    public static final String COMPUTATION_SERIALIZED_NAME = "COMPUTATION";
    public static final String HAVING_SERIALIZED_NAME = "HAVING";
    public static final String CULLING_SERIALIZED_NAME = "CULLING";

    /** Represents the type of the PostAggregation. */
    public enum Type {
        @SerializedName(ORDER_BY_SERIALIZED_NAME)
        ORDER_BY,
        @SerializedName(COMPUTATION_SERIALIZED_NAME)
        COMPUTATION,
        @SerializedName(HAVING_SERIALIZED_NAME)
        HAVING,
        @SerializedName(CULLING_SERIALIZED_NAME)
        CULLING
    }

    @Expose @SerializedName(TYPE_FIELD)
    protected Type type;

    public static final String TYPE_FIELD = "type";
    public static final BulletError TYPE_MISSING =
            makeError("Missing post-aggregation type", "You must specify a type for post-aggregation");

    @Override
    public Optional<List<BulletError>> initialize() {
        if (type == null) {
            return Optional.of(singletonList(TYPE_MISSING));
        }
        return Optional.empty();
    }
}
