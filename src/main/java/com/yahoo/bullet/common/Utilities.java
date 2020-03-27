/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Utilities {
    private static final String KEY_DELIMITER = "\\.";

   /**
    * Tries to get the object casted as the target type. If it is generic, the captured types cannot not be
    * validated. Only the base object type is validated.
    *
    * @param entry The object to cast.
    * @param clazz The class of the U.
    * @param <U> The type to get the object as.
    * @return The casted object of type U or null if the cast could not be done.
    */
    @SuppressWarnings("unchecked")
    public static <U> U getCasted(Object entry, Class<U> clazz) {
        try {
            return clazz.cast(entry);
        } catch (ClassCastException ignored) {
        }
        return null;
    }

    /**
     * Tries to get a key from a map as the target type. If it is a generic type, the captured types are not
     * validated. Only the base object type is validated.
     *
     * @param map  The non-null map that possibly contains the key.
     * @param key  The String key to get the value for from the map.
     * @param clazz The class of the U.
     * @param <U> The type to get the object as.
     * @return The casted object of type U or null if the cast could not be done.
     * @throws NullPointerException if the map was null.
     */
    @SuppressWarnings("unchecked")
    public static <U> U getCasted(Map<String, Object> map, String key, Class<U> clazz) {
        return getCasted(map.get(key), clazz);
    }

    /**
     * Checks to see if the {@link Map} contains any mappings.
     *
     * @param map The map to check.
     * @return A boolean denoting whether the map had mappings.
     */
    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    /**
     * Checks to see if the {@link Collection} contains any items.
     *
     * @param collection The collection to check.
     * @return A boolean denoting whether the collection had items.
     */
    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    /**
     * Checks to see if the {@link String} is present.
     *
     * @param string The string to check.
     * @return A boolean denoting whether the string was present.
     */
    public static boolean isEmpty(String string) {
        return string == null || string.isEmpty();
    }

    /**
     * Rounds a double up to the specified number of places.
     *
     * @param value The number to round.
     * @param places The number of maximum decimal places to round up to.
     * @return The resulting rounded double.
     */
    public static double round(double value, int places) {
        return Double.isInfinite(value) || Double.isNaN(value) ?
               value : BigDecimal.valueOf(value).setScale(places, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * Gets a field from the record by identifier.
     *
     * For example, suppose a record has a map of boolean maps called "aaa". Then
     * - "aaa" identifies that map of maps
     * - "aaa.bbb" identifies the inner map that "aaa" maps "bbb" to (if it exists)
     * - "aaa.bbb.ccc" identifies the boolean that "aaa.bbb" (if it exists) maps "ccc" to (if it exists)
     *
     * For a list element, the index is the key, e.g. "my_list.0" or "my_list.0.some_key"
     *
     * @param field The non-null identifier of the field to get.
     * @return The value of the field or null if it does not exist.
     */
    public static TypedObject extractField(String field, BulletRecord record) {
        String[] keys = field.split(KEY_DELIMITER, 3);
        TypedObject first = record.typedGet(keys[0]);
        if (keys.length == 1) {
            return first;
        }
        Object second;
        if (first.isMap()) {
            second = ((Map) first.getValue()).get(keys[1]);
        } else if (first.isList()) {
            second = ((List) first.getValue()).get(Integer.parseInt(keys[1]));
        } else {
            return TypedObject.NULL;
        }
        if (second == null) {
            return TypedObject.NULL;
        }
        if (keys.length == 2) {
            return new TypedObject(first.getType().getSubType(), second);
        }
        if (!first.isComplexMap() && !first.isComplexList()) {
            return TypedObject.NULL;
        }
        Object third = ((Map) second).get(keys[2]);
        return third != null ? new TypedObject(first.getType().getSubType().getSubType(), third) : TypedObject.NULL;
    }

    /**
     * Extracts the field from the given (@link BulletRecord} as a {@link Number}, if possible.
     *
     * @param field The field to get as a number.
     * @param record The record containing the field.
     * @return The value of the field as a {@link Number} or null if it cannot be forced to one.
     */
    public static Number extractFieldAsNumber(String field, BulletRecord record) {
        TypedObject value = record.typedGet(field);
        if (value.isNull()) {
            return null;
        }
        if (Type.isNumeric(value.getType())) {
            return (Number) value.getValue();
        }
        try {
            return (Number) value.forceCast(Type.DOUBLE).getValue();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * This method loads a given class name with the class name key and creates an instance of it by using a constructor
     * that has a single argument for a {@link BulletConfig}. It then passes in the provided config and returns the
     * constructed instance.
     *
     * @param name The name of class name to load.
     * @param config The {@link BulletConfig} to use to create an instance of the class.
     * @param <S> The type of the class.
     * @return A created instance of this class.
     * @throws RuntimeException if there were issues creating an instance. It wraps the real exception.
     */
    @SuppressWarnings("unchecked")
    public static <S> S loadConfiguredClass(String name, BulletConfig config) {
        try {
            Class<? extends S> className = (Class<? extends S>) Class.forName(name);
            Constructor<? extends S> constructor = className.getConstructor(BulletConfig.class);
            return constructor.newInstance(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
