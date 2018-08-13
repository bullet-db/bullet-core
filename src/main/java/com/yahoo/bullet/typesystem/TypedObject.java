/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.typesystem;

import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

@Getter
public class TypedObject implements Comparable<TypedObject> {
    private final Type type;
    // value is undefined if type is Type.UNKNOWN
    private final Object value;

    // The inside primitive type for a List or Map. This field is only used for LIST or MAP.
    private final Type primitiveType;

    public static final Predicate<TypedObject> IS_PRIMITIVES_OR_NULL = (t) -> t.getType() == Type.NULL || Type.PRIMITIVES.contains(t.getType());
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
        primitiveType = extractPrimitiveType(type, value);
    }

    /**
     * Takes a String value and returns a casted TypedObject according to this type.
     *
     * @param type The {@link Type} to cast the values to.
     * @param value The string value that is being cast.
     * @return The casted TypedObject with the type set to the appropriate {@link Type} or
     * {@link TypedObject#GENERIC_UNKNOWN} if it cannot.
     */
    public static TypedObject typeCast(Type type, String value) {
        try {
            return new TypedObject(type, type.cast(value));
        } catch (RuntimeException e) {
            return GENERIC_UNKNOWN;
        }
    }

    /**
     * Takes an object and returns a casted TypedObject according to this type.
     *
     * @param type The {@link Type} to cast the values to.
     * @param object The Object that is being cast.
     * @return The casted TypedObject with the type set to the appropriate {@link Type} or
     *         {@link TypedObject#GENERIC_UNKNOWN} if it cannot.
     */
    public static TypedObject typeCastFromObject(Type type, Object object) {
        if (object == null) {
            return GENERIC_UNKNOWN;
        }
        try {
            return new TypedObject(type, type.castObject(object));
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

    /**
     * Get the size of the value. Currently only LIST, MAP ans STRING are supported.
     *
     * @return the size of the value.
     * @throws RuntimeException if not supported.
     */
    public Integer sizeOf() {
        switch (type) {
            case LIST:
                return List.class.cast(value).size();
            case MAP:
                return Map.class.cast(value).size();
            case STRING:
                return String.class.cast(value).length();
            default:
                throw new RuntimeException("Unsupported type cannot be operated SIZEOF: " + type);
        }
    }

    /**
     * Returns true if the value or its underneath values contain a mapping for the specified key. Only LIST and MAP are supported.
     *
     * @param key The key to be tested.
     * @return A Boolean to indicate if the value or its underneath values contain a mapping for the specified key.
     * @throws RuntimeException if not supported.
     */
    @SuppressWarnings("unchecked")
    public Boolean containsKey(String key) {
        switch (type) {
            case LIST:
                for (Object element : (List) value) {
                    if (element instanceof Map) {
                        if (((Map) element).containsKey(key)) {
                            return true;
                        }
                    }
                }
                return false;
            case MAP:
                Map<String, ?> map = (Map) value;
                if (map.containsKey(key)) {
                    return true;
                }
                for (Map.Entry entry : map.entrySet()) {
                    if (entry.getValue() instanceof Map) {
                        if (((Map) entry.getValue()).containsKey(key)) {
                            return true;
                        }
                    }
                }
                return false;
            default:
                throw new RuntimeException("Unsupported type cannot be operated CONTAINSKEY: " + type);
        }
    }

    /**
     * Returns true if the value or its underneath values contain the specified value. Only LIST and MAP are supported.
     *
     * @param target The target {@link TypedObject} to be tested.
     * @return A Boolean to indicate if the value or its underneath values contain the specified value.
     * @throws RuntimeException if not supported.
     */
    @SuppressWarnings("unchecked")
    public Boolean containsValue(TypedObject target) {
        switch (type) {
            case LIST:
                for (Object element : (List) value) {
                    if (element instanceof Map) {
                        if (containsValueInPrimitiveMap((Map) element, target)) {
                            return true;
                        }
                    }
                    // Support list of primitives after https://github.com/bullet-db/bullet-record/issues/12 is done.
                }
                return false;
            case MAP:
                for (Map.Entry entry : ((Map<?, ?>) value).entrySet()) {
                    Object entryValue = entry.getValue();
                    if (entryValue instanceof Map) {
                        if (containsValueInPrimitiveMap((Map) entryValue, target)) {
                            return true;
                        }
                    } else {
                        TypedObject typedObject = new TypedObject(entryValue);
                        if (typedObject.compareTo(target) == 0) {
                            return true;
                        }

                    }
                }
                return false;
            default:
                throw new RuntimeException("Unsupported type cannot be operated CONTAINSVALUE: " + type);
        }
    }

    @Override
    public String toString() {
        return type == Type.NULL ? Type.NULL_EXPRESSION : value.toString();
    }

    private static Boolean containsValueInPrimitiveMap(Map<?, ?> map, TypedObject target) {
        for (Map.Entry entry : map.entrySet()) {
            TypedObject typedObject = new TypedObject(entry.getValue());
            if (typedObject.compareTo(target) == 0) {
                return true;
            }
        }
        return false;
    }

    private static Type extractPrimitiveTypeFromMap(Map map) {
        if (map.isEmpty()) {
            return Type.UNKNOWN;
        }
        Object firstValue = map.get(map.keySet().iterator().next());
        return Type.getType(firstValue);

    }

    @SuppressWarnings("unchecked")
    private static Type extractPrimitiveType(Type type, Object target) {
        try {
            switch (type) {
                case LIST:
                    List list = (List) target;
                    if (list.isEmpty()) {
                        return Type.UNKNOWN;
                    }
                    if (list.get(0) instanceof Map) {
                        return extractPrimitiveTypeFromMap((Map) list.get(0));
                    }
                    return Type.getType(list.get(0));
                case MAP:
                    Map map = (Map) target;
                    if (map.isEmpty()) {
                        return Type.UNKNOWN;
                    }
                    Object firstValue = map.get(map.keySet().iterator().next());
                    if (firstValue instanceof Map) {
                        return extractPrimitiveTypeFromMap((Map) firstValue);
                    }
                    return Type.getType(firstValue);
                default:
                    return Type.UNKNOWN;
            }
        } catch (RuntimeException e) {
            return Type.UNKNOWN;
        }
    }
}

