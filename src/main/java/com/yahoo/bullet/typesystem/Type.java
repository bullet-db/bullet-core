/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.typesystem;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Getter
public enum Type {
    STRING(String.class),
    BOOLEAN(Boolean.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    LIST(List.class),
    MAP(Map.class),
    // Doesn't matter what underlyingType is for NULL and UNKNOWN, just need something that isn't encountered
    NULL(Type.class),
    UNKNOWN(Type.class);

    public static final String NULL_EXPRESSION = "null";
    public static List<Type> PRIMITIVES = Arrays.asList(BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, STRING);
    public static List<Type> NUMERICS = Arrays.asList(INTEGER, LONG, FLOAT, DOUBLE);

    private final Class underlyingType;

    /**
     * Constructor.
     *
     * @param underlyingType The Java type that this type represents.
     */
    Type(Class underlyingType) {
        this.underlyingType = underlyingType;
    }

    /**
     * Tries to get the type of a given object from {@link #PRIMITIVES}.
     *
     * @param object The object whose type is to be determined.
     * @return {@link Type} for this object, the {@link Type#NULL} if the object was null or {@link Type#UNKNOWN}
     *         if the type could not be determined.
     */
    public static Type getType(Object object) {
        if (object == null) {
            return Type.NULL;
        }

        // Only support the atomic, primitive types for now since all our operations are on atomic types.
        for (Type type : PRIMITIVES) {
            if (type.getUnderlyingType().isInstance(object)) {
                return type;
            }
        }
        return UNKNOWN;
    }

    /**
     * Takes a value and casts it to this type.
     *
     * @param value The string value that is being cast.
     * @return The casted object.
     * @throws RuntimeException if the cast cannot be done.
     */
    public Object cast(String value) {
        switch (this) {
            case BOOLEAN:
                return Boolean.valueOf(value);
            case INTEGER:
                return Integer.valueOf(value);
            case LONG:
                // If we want to allow decimals to be casted as longs, do Double.valueOf(value).longValue() instead
                return Long.valueOf(value);
            case FLOAT:
                return Float.valueOf(value);
            case DOUBLE:
                return Double.valueOf(value);
            case STRING:
                return value;
            case NULL:
                return value == null || NULL_EXPRESSION.compareToIgnoreCase(value) == 0 ? null : value;
            case UNKNOWN:
                return value;
            // We won't support the rest for castability. This wouldn't happen if getType was used to create
            // TypedObjects because because we only support PRIMITIVES and UNKNOWN
            default:
                throw new ClassCastException("Cannot cast " + value + " to type " + this);
        }
    }

}

