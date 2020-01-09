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
    LISTOFMAP(List.class),
    MAPOFMAP(Map.class),
    // Doesn't matter what underlyingType is for NULL and UNKNOWN, just need something that isn't encountered
    NULL(Type.class),
    UNKNOWN(Type.class);

    public static final String NULL_EXPRESSION = "null";
    public static List<Type> PRIMITIVES = Arrays.asList(BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, STRING);
    public static List<Type> COLLECTIONS = Arrays.asList(LIST, MAP, LISTOFMAP, MAPOFMAP);
    public static List<Type> SUPPORTED_TYPES = Arrays.asList(BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, STRING, LIST, MAP);
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
     * Tries to get the type of a given object from {@link #SUPPORTED_TYPES}.
     *
     * @param object The object whose type is to be determined.
     * @return {@link Type} for this object, the {@link Type#NULL} if the object was null or {@link Type#UNKNOWN}
     *         if the type could not be determined.
     */
    public static Type getType(Object object) {
        if (object == null) {
            return Type.NULL;
        }
        for (Type type : SUPPORTED_TYPES) {
            if (type.getUnderlyingType().isInstance(object)) {
                switch (type) {
                    case LIST:
                        return containsMap((List) object) ? LISTOFMAP : LIST;
                    case MAP:
                        return containsMap((Map) object) ? MAPOFMAP : MAP;
                }
                return type;
            }
        }
        return UNKNOWN;
    }

    private static boolean containsMap(List target) {
        return !target.isEmpty() && target.get(0) instanceof Map;
    }

    private static boolean containsMap(Map target) {
        return !target.isEmpty() && target.values().stream().findAny().get() instanceof Map;
    }

    /**
     * Checks to see if a given string is the {@link #NULL_EXPRESSION}.
     *
     * @param string The string to check if it is null.
     * @return A boolean denoting whether the given string represented a null.
     */
    public static boolean isNullExpression(String string) {
        return NULL_EXPRESSION.compareToIgnoreCase(string) == 0;
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
                return value == null || isNullExpression(value) ? null : value;
            // We won't support the rest for castability. This wouldn't happen if getType was used to create
            // TypedObjects because because we only support cast operation on PRIMITIVES and NULL.
            default:
                throw new ClassCastException("Cannot cast " + value + " to type " + this);
        }
    }

    /**
     * Takes an object and casts it to this type.
     *
     * @param object The object that is being cast.
     * @return The casted object.
     * @throws ClassCastException if the cast cannot be done.
     */
    public Object castObject(Object object) throws ClassCastException {
        if (this == LONG && object instanceof Integer) {
            return ((Integer) object).longValue();
        } else if (this == DOUBLE && object instanceof Float) {
            return ((Float) object).doubleValue();
        } else {
            return underlyingType.cast(object);
        }
    }

    /**
     * Force cast the Object to given {@link Type} castedType.
     *
     * @param castedType The {@link Type} to be casted to.
     * @param object The object to be casted.
     * @return The {@link Object} to be casted.
     */
    public Object forceCast(Type castedType, Object object) {
        switch (castedType) {
            case INTEGER:
                return castToInteger(object);
            case BOOLEAN:
                return castToBoolean(object);
            case STRING:
                return object.toString();
            case LONG:
                return castToLong(object);
            case FLOAT:
                return castToFloat(object);
            case DOUBLE:
                return castToDouble(object);
            default:
                throw new UnsupportedOperationException("Unsupported type cannot be casted: " + castedType);
        }
    }

    private Integer castToInteger(Object object) {
        switch (this) {
            case INTEGER:
                return (Integer) object;
            case LONG:
                return ((Long) object).intValue();
            case FLOAT:
                return (int) (((Float) object).floatValue());
            case DOUBLE:
                return (int) (((Double) object).doubleValue());
            case STRING:
                return (int) (Double.parseDouble((String) object));
            case BOOLEAN:
                return ((Boolean) object) ? 1 : 0;
            default:
                throw new UnsupportedOperationException("Can not cast to Integer from Type: " + this);
        }
    }

    private Long castToLong(Object object) {
        switch (this) {
            case INTEGER:
                return ((Integer) object).longValue();
            case LONG:
                return (Long) object;
            case FLOAT:
                return (long) (((Float) object).floatValue());
            case DOUBLE:
                return (long) (((Double) object).doubleValue());
            case STRING:
                return (long) (Double.parseDouble((String) object));
            case BOOLEAN:
                return ((Boolean) object) ? 1L : 0L;
            default:
                throw new UnsupportedOperationException("Can not cast to Long from Type: " + this);
        }
    }

    private Float castToFloat(Object object) {
        switch (this) {
            case INTEGER:
                return (float) (Integer) object;
            case LONG:
                return (float) (Long) object;
            case FLOAT:
                return (Float) object;
            case DOUBLE:
                return ((Double) object).floatValue();
            case STRING:
                return (float) (Double.parseDouble((String) object));
            case BOOLEAN:
                return ((Boolean) object) ? 1.0f : 0.0f;
            default:
                throw new UnsupportedOperationException("Can not cast to Float from Type: " + this);
        }
    }

    private Double castToDouble(Object object) {
        switch (this) {
            case INTEGER:
                return (double) (Integer) object;
            case LONG:
                return (double) (Long) object;
            case FLOAT:
                return ((Float) object).doubleValue();
            case DOUBLE:
                return (Double) object;
            case STRING:
                return Double.parseDouble((String) object);
            case BOOLEAN:
                return ((Boolean) object) ? 1.0 : 0.0;
            default:
                throw new UnsupportedOperationException("Can not cast to Double from Type: " + this);
        }
    }

    private Boolean castToBoolean(Object object) {
        switch (this) {
            case INTEGER:
                return (Integer) object != 0;
            case LONG:
                return ((Long) object) != 0;
            case FLOAT:
                return ((Float) object) != 0;
            case DOUBLE:
                return ((Double) object) != 0;
            case STRING:
                return Boolean.parseBoolean((String) object);
            case BOOLEAN:
                return (Boolean) object;
            default:
                throw new UnsupportedOperationException("Can not cast to Boolean from Type: " + this);
        }
    }
}

