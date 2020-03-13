/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Configurable;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter @Setter
public class Aggregation implements Configurable {
    /** Represents the type of the Aggregation. */
    public enum Type {
        // The alternate value of DISTINCT for GROUP is allowed since having no GROUP operations is implicitly
        // a DISTINCT
        GROUP,
        COUNT_DISTINCT,
        TOP_K,
        DISTRIBUTION,
        // The alternate value of LIMIT for RAW is allowed to preserve backward compatibility.
        RAW
    }

    private Integer size;
    private Type type;
    private Map<String, Object> attributes;
    private Map<String, String> fields;

    /**
     * Default constructor.
     */
    public Aggregation() {
        type = Type.RAW;
    }

    public Aggregation(Integer size, Type type) {
        this.size = size;
        this.type = type;
    }

    @Override
    public void configure(BulletConfig config) {
        int sizeDefault = config.getAs(BulletConfig.AGGREGATION_DEFAULT_SIZE, Integer.class);
        int sizeMaximum = config.getAs(BulletConfig.AGGREGATION_MAX_SIZE, Integer.class);

        // Null or not positive, then default, else min of size and max
        size = (size == null || size <= 0) ? sizeDefault : Math.min(size, sizeMaximum);
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", attributes: " + attributes + "}";
    }
}
