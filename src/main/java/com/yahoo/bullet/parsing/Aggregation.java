/*
 *  Copyright 2017, Yahoo Inc.
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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.common.BulletError.makeError;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Getter @Setter
public class Aggregation implements Configurable, Initializable {
    /** Represents the type of the Aggregation. */
    public enum Type {
        // The alternate value of DISTINCT for GROUP is allowed since having no GROUP operations is implicitly
        // a DISTINCT
        @SerializedName(value = "GROUP", alternate = { "DISTINCT" })
        GROUP,
        @SerializedName("COUNT DISTINCT")
        COUNT_DISTINCT,
        @SerializedName("TOP K")
        TOP_K,
        @SerializedName("DISTRIBUTION")
        DISTRIBUTION,
        // The alternate value of LIMIT for RAW is allowed to preserve backward compatibility.
        @SerializedName(value = "RAW", alternate = { "LIMIT" })
        RAW
    }

    @Expose
    private Integer size;
    @Expose
    private Type type;
    @Expose
    private Map<String, Object> attributes;
    @Expose
    private Map<String, String> fields;

    public static final Set<Type> SUPPORTED_AGGREGATION_TYPES =
            new HashSet<>(asList(Type.GROUP, Type.COUNT_DISTINCT, Type.RAW, Type.DISTRIBUTION, Type.TOP_K));
    public static final BulletError TYPE_NOT_SUPPORTED_ERROR =
            makeError("Unknown aggregation type", "Current supported aggregation types are: RAW (or LIMIT), " +
                                                  "GROUP (or DISTINCT), COUNT DISTINCT, DISTRIBUTION, TOP K");

    /**
     * Default constructor. GSON recommended
     */
    public Aggregation() {
        type = Type.RAW;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(BulletConfig config) {
        int sizeDefault = config.getAs(BulletConfig.AGGREGATION_DEFAULT_SIZE, Integer.class);
        int sizeMaximum = config.getAs(BulletConfig.AGGREGATION_MAX_SIZE, Integer.class);

        // Null or not positive, then default, else min of size and max
        size = (size == null || size <= 0) ? sizeDefault : Math.min(size, sizeMaximum);
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        // Includes null
        return SUPPORTED_AGGREGATION_TYPES.contains(type) ? Optional.empty() :
                                                            Optional.of(singletonList(TYPE_NOT_SUPPORTED_ERROR));
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", attributes: " + attributes + "}";
    }
}
