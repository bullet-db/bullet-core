/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.typesystem;

import lombok.Getter;

import java.util.Objects;
import java.util.function.Predicate;

@Getter
public class TypedObject implements Comparable<TypedObject> {
    private final Type type;
    // value is undefined if type is Type.UNKNOWN
    private final Object value;

    public static final Predicate<TypedObject> IS_NOT_UNKNOWN = (t) -> t.getType() != Type.UNKNOWN;
    public static final Predicate<TypedObject> IS_NOT_NULL = (t) -> t.getType() != Type.NULL;
    public static final TypedObject GENERIC_UNKNOWN = new TypedObject(Type.UNKNOWN, null);

    /**
     * Constructor that wraps an Object into a type.
     *
     * @param value The value who is being wrapped.
     */
    public TypedObject(Object value) {
        this(Type.getType(value), value);
    }

    /**
     * Create a TypedObject with the given non-null type.
     *
     * @param type The type of the value.
     * @param value The payload.
     */
    public TypedObject(Type type, Object value) {
        Objects.requireNonNull(type);
        this.type = type;
        this.value = value;
    }

    /**
     * Takes a String value and returns a casted TypedObject according to this type.
     *
     * @param value The string value that is being cast.
     * @return The casted TypedObject with the type set to the appropriate {@link Type} or
     *         {@link TypedObject#GENERIC_UNKNOWN} if it cannot.
     */
    public TypedObject typeCast(String value) {
        try {
            return new TypedObject(type, type.cast(value));
        } catch (RuntimeException e) {
            return GENERIC_UNKNOWN;
        }
    }

    /**
     * Takes a non-null value and returns a numeric TypedObject - it has a type in {@link Type#NUMERICS}. The value
     * is then a {@link Number}. It uses the String representation of the object to cast it.
     *
     * @param value The Object value that is being cast to a numeric.
     * @return The casted TypedObject with the type set to numeric or {@link TypedObject#GENERIC_UNKNOWN} if not.
     */
    public static TypedObject makeNumber(Object value) {
        if (value == null) {
            return GENERIC_UNKNOWN;
        }
        try {
            return new TypedObject(Type.DOUBLE, Type.DOUBLE.cast(value.toString()));
        } catch (RuntimeException e) {
            return GENERIC_UNKNOWN;
        }
    }

    /**
     * Compares this TypedObject to another. If the value is null and the other isn't,
     * returns Integer.MIN_VALUE. If this TypedObject is unknown, returns Integer.MIN_VALUE.
     * {@inheritDoc}
     *
     * @param o The other non-null TypedObject
     * @return {@inheritDoc}
     */
    @Override
    public int compareTo(TypedObject o) {
        Objects.requireNonNull(o);
        // If type casting/unification needs to happen, it should go here. Assume this.type == o.type for now
        switch (type) {
            case STRING:
                return value.toString().compareTo((String) o.value);
            case BOOLEAN:
                return ((Boolean) value).compareTo((Boolean) o.value);
            case INTEGER:
                return ((Integer) value).compareTo((Integer) o.value);
            case LONG:
                return ((Long) value).compareTo((Long) o.value);
            case FLOAT:
                return ((Float) value).compareTo((Float) o.value);
            case DOUBLE:
                return ((Double) value).compareTo((Double) o.value);
            case NULL:
                // Return Integer.MIN_VALUE if the type isn't null. We could throw an exception instead.
                return o.value == null ? 0 : Integer.MIN_VALUE;
            case UNKNOWN:
                return Integer.MIN_VALUE;
            default:
                throw new RuntimeException("Unsupported type cannot be compared: " + type);
        }
    }

    @Override
    public String toString() {
        return type == Type.NULL ? Type.NULL_EXPRESSION : value.toString();
    }
}

