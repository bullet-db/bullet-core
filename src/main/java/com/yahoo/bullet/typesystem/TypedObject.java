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
    // The nested primitive type for a List or Map. This field is only used for LIST or MAP.
    private final Type primitiveType;
    // value is undefined if type is Type.UNKNOWN
    private final Object value;

    public static final Predicate<TypedObject> IS_PRIMITIVE_OR_NULL = (t) -> t.getType() == Type.NULL || Type.PRIMITIVES.contains(t.getType());
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
     * @param value The string value that is being cast.
     * @return The casted TypedObject with the type set to the appropriate {@link Type} or
     * {@link TypedObject#GENERIC_UNKNOWN} if it cannot.
     */
    public TypedObject typeCast(String value) {
        return typeCast(type, value);
    }

    /**
     * Takes an object and returns a casted TypedObject according to this type.
     *
     * @param object The Object that is being cast.
     * @return The casted TypedObject with the type set to the appropriate {@link Type} or
     *         {@link TypedObject#GENERIC_UNKNOWN} if it cannot.
     */
    public TypedObject typeCastFromObject(Object object) {
        return typeCastFromObject(type, object);
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
     * Force cast to the {@link TypedObject} with given {@link Type}.
     *
     * @param castedType The {@link Type} to be casted to
     * @return The casted {@link TypedObject}
     */
    public TypedObject forceCast(Type castedType) {
        switch (castedType) {
            case INTEGER:
                return new TypedObject(castedType, castToInteger(this));
            case BOOLEAN:
                return new TypedObject(castedType, castToBoolean(this));
            case STRING:
                return new TypedObject(castedType, value.toString());
            case LONG:
                return new TypedObject(castedType, castToLong(this));
            case FLOAT:
                return new TypedObject(castedType, castToFloat(this));
            case DOUBLE:
                return new TypedObject(castedType, castToDouble(this));
            default:
                throw new UnsupportedOperationException("Unsupported type cannot be casted: " + castedType);
        }
    }

    /**
     * Get the size of the value. Currently only LIST, MAP and STRING are supported.
     *
     * @return the size of the value.
     * @throws UnsupportedOperationException if not supported.
     */
    public int size() {
        switch (type) {
            case LIST:
                return List.class.cast(value).size();
            case MAP:
                return Map.class.cast(value).size();
            case STRING:
                return String.class.cast(value).length();
            default:
                throw new UnsupportedOperationException("This type of field does not support size of: " + type);
        }
    }

    /**
     * Returns true if the value or its underlying values contain a mapping for the specified key. Only LIST and MAP are supported.
     *
     * @param key The key to be tested.
     * @return A Boolean to indicate if the value or its underlying values contain a mapping for the specified key.
     * @throws UnsupportedOperationException if not supported.
     */
    @SuppressWarnings("unchecked")
    public boolean containsKey(String key) {
        switch (type) {
            case LIST:
                return ((List) value).stream().anyMatch(e -> e instanceof Map && ((Map) e).containsKey(key));
            case MAP:
                Map map = (Map) value;
                return map.containsKey(key) || map.values().stream().anyMatch(e -> e instanceof Map && ((Map) e).containsKey(key));
            default:
                throw new UnsupportedOperationException("This type of field does not support contains key: " + type);
        }
    }

    /**
     * Returns true if the value or its underlying values contain the specified value. Only LIST and MAP are supported.
     *
     * @param target The target {@link TypedObject} to be tested.
     * @return A Boolean to indicate if the value or its underlying values contain the specified value.
     * @throws UnsupportedOperationException if not supported.
     */
    @SuppressWarnings("unchecked")
    public boolean containsValue(TypedObject target) {
        switch (type) {
            case LIST:
                return ((List) value).stream().anyMatch(e -> e instanceof Map ? containsValueInPrimitiveMap((Map) e, target) : target.equalTo(e));
            case MAP:
                Map map = (Map) value;
                return map.values().stream().anyMatch(e -> e instanceof Map ? containsValueInPrimitiveMap((Map) e, target) : target.equalTo(e));
            default:
                throw new UnsupportedOperationException("This type of field does not support contains value: " + type);
        }
    }

    @Override
    public String toString() {
        return type == Type.NULL ? Type.NULL_EXPRESSION : value.toString();
    }

    /**
     * Returns true if this equals the specified object. The object can be a {@link TypedObject} or be constructed to a {@link TypedObject}.
     *
     * @param object The object to compare to.
     * @return A boolean to indicate if this equals the specified object.
     */
    public boolean equalTo(Object object) {
        TypedObject target = object instanceof TypedObject ? (TypedObject) object : new TypedObject(object);
        return compareTo(target) == 0;
    }

    private static boolean containsValueInPrimitiveMap(Map<?, ?> map, TypedObject target) {
        return map.values().stream().anyMatch(target::equalTo);
    }

    private static Type extractPrimitiveType(Type type, Object target) {
        switch (type) {
            case LIST:
                return extractPrimitiveTypeFromList((List) target);
            case MAP:
                return extractPrimitiveTypeFromMap((Map) target);
            default:
                return Type.UNKNOWN;
        }
    }

    private static Type extractPrimitiveTypeFromList(List list) {
        if (list.isEmpty()) {
            return Type.UNKNOWN;
        }
        Object firstElement = list.get(0);
        if (firstElement instanceof Map) {
            return extractPrimitiveTypeFromMap((Map) firstElement);
        }
        return Type.getType(firstElement);
    }

    private static Type extractPrimitiveTypeFromMap(Map map) {
        if (map.isEmpty()) {
            return Type.UNKNOWN;
        }
        Object firstValue = map.values().stream().findAny().get();
        if (firstValue instanceof Map) {
            Map innerMap = (Map) firstValue;
            return innerMap.isEmpty() ? Type.UNKNOWN : Type.getType(innerMap.values().stream().findAny().get());
        }
        return Type.getType(firstValue);
    }


    private Integer castToInteger(TypedObject object) {
        switch (object.getType()) {
            case INTEGER:
                return (Integer) object.getValue();
            case LONG:
                return ((Long) object.getValue()).intValue();
            case FLOAT:
                return (int) (((Float) object.getValue()).floatValue());
            case DOUBLE:
                return (int) (((Double) object.getValue()).doubleValue());
            case STRING:
                return (int) (Double.parseDouble((String) object.getValue()));
            case BOOLEAN:
                return ((Boolean) object.getValue()) ? 1 : 0;
            default:
                throw new UnsupportedOperationException("Can not cast to Integer from Type: " + object.getType());
        }
    }

    private Long castToLong(TypedObject object) {
        switch (object.getType()) {
            case INTEGER:
                return ((Integer) object.getValue()).longValue();
            case LONG:
                return (Long) object.getValue();
            case FLOAT:
                return (long) (((Float) object.getValue()).floatValue());
            case DOUBLE:
                return (long) (((Double) object.getValue()).doubleValue());
            case STRING:
                return (long) (Double.parseDouble((String) object.getValue()));
            case BOOLEAN:
                return ((Boolean) object.getValue()) ? 1L : 0L;
            default:
                throw new UnsupportedOperationException("Can not cast to Long from Type: " + object.getType());
        }
    }

    private Float castToFloat(TypedObject object) {
        switch (object.getType()) {
            case INTEGER:
                return (float) (Integer) object.getValue();
            case LONG:
                return (float) (Long) object.getValue();
            case FLOAT:
                return (Float) object.getValue();
            case DOUBLE:
                return ((Double) object.getValue()).floatValue();
            case STRING:
                return (float) (Double.parseDouble((String) object.getValue()));
            case BOOLEAN:
                return ((Boolean) object.getValue()) ? 1.0f : 0.0f;
            default:
                throw new UnsupportedOperationException("Can not cast to Long from Type: " + object.getType());
        }
    }

    private Double castToDouble(TypedObject object) {
        switch (object.getType()) {
            case INTEGER:
                return (double) (Integer) object.getValue();
            case LONG:
                return (double) (Long) object.getValue();
            case FLOAT:
                return ((Float) object.getValue()).doubleValue();
            case DOUBLE:
                return (Double) object.getValue();
            case STRING:
                return Double.parseDouble((String) object.getValue());
            case BOOLEAN:
                return ((Boolean) object.getValue()) ? 1.0 : 0.0;
            default:
                throw new UnsupportedOperationException("Can not cast to Long from Type: " + object.getType());
        }
    }

    private Boolean castToBoolean(TypedObject object) {
        switch (object.getType()) {
            case INTEGER:
                return (Integer) object.getValue() != 0;
            case LONG:
                return ((Long) object.getValue()) != 0;
            case FLOAT:
                return ((Float) object.getValue()) != 0;
            case DOUBLE:
                return ((Double) object.getValue()) != 0;
            case STRING:
                return Boolean.parseBoolean((String) object.getValue());
            case BOOLEAN:
                return (Boolean) object.getValue();
            default:
                throw new UnsupportedOperationException("Can not cast to Long from Type: " + object.getType());
        }
    }
}
