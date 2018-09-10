/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;
import static java.util.Collections.singletonList;

@Getter @Setter
public abstract class PostAggregation implements Configurable, Initializable {
    /** Represents the type of the PostAggregation. */
    public enum Type {
        @SerializedName("ORDERBY")
        ORDER_BY,
        @SerializedName("COMPUTATION")
        COMPUTATION
    }

    @Expose @SerializedName(TYPE_FIELD)
    protected Type type;

    public static final String TYPE_FIELD = "type";
    public static final BulletError TYPE_MISSING =
            makeError("Missing post aggregation type", "You must specify a type for post aggregation");

    /**
     * Default constructor. GSON recommended
     */
    public PostAggregation() {
        type = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (type == null) {
            return Optional.of(singletonList(TYPE_MISSING));
        }
        return Optional.empty();
    }

    @Override
    public void configure(BulletConfig configuration) {
    }
}
