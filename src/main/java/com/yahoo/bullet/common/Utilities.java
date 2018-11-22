/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Map;

public class Utilities {
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
     * Extracts this identifier as a {@link TypedObject}.
     *
     * @param identifier The identifier name to extract. It can be "." separated to look inside maps and arrays.
     * @param record The {@link BulletRecord} to extract it from.
     * @return The created TypedObject from the value for the identifier in the record.
     */
    public static TypedObject extractTypedObject(String identifier, BulletRecord record) {
        return new TypedObject(record.extractField(identifier));
    }

    /**
     * Extracts the identifier from the given (@link BulletRecord} as a {@link Number}, if possible.
     *
     * @param identifier The identifier of a number to get. It can be "." separated to look inside maps and arrays.
     * @param record The record containing the identifier.
     * @return The value of the identifier as a {@link Number} or null if it cannot be forced to one.
     */
    public static Number extractFieldAsNumber(String identifier, BulletRecord record) {
        Object value = record.extractField(identifier);
        // Also checks for null
        if (value instanceof Number) {
            return (Number) value;
        }
        TypedObject asNumber = TypedObject.makeNumber(value);
        if (asNumber.getType() == Type.UNKNOWN) {
            return null;
        }
        return (Number) asNumber.getValue();
    }
}
