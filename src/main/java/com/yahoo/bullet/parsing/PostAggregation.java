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
import java.util.Map;
import java.util.Optional;

@Getter @Setter
public class PostAggregation implements Configurable, Initializable {
    /** Represents the type of the PostAggregation. */
    public enum Type {
        @SerializedName("ORDERBY")
        ORDER_BY,
        @SerializedName("COMPUTATION")
        COMPUTATION

    }

    @Expose
    protected Type type;
    @Expose
    private Map<String, Object> attributes;

    /**
     * Default constructor. GSON recommended
     */
    public PostAggregation() {
        type = Type.ORDER_BY;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }

    @Override
    public void configure(BulletConfig configuration) {

    }

    @Override
    public String toString() {
        return "{type: " + type + ", attributes: " + attributes + "}";
    }
}
