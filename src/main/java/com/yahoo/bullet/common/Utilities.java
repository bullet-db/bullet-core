/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
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
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

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
     * Throws a {@link NullPointerException} if the {@link List} is null or contains any null elements.
     *
     * @param list The list to check.
     * @param <T> The type of the list.
     * @return The list.
     */
    public static <T> List<T> requireNonNull(List<T> list) {
        Objects.requireNonNull(list);
        for (T t : list) {
            Objects.requireNonNull(t);
        }
        return list;
    }

    /**
     * Throws a {@link NullPointerException} if the {@link Set} is null or contains any null elements.
     *
     * @param set The set to check.
     * @param <T> The type of the set.
     * @return The set.
     */
    public static <T> Set<T> requireNonNull(Set<T> set) {
        Objects.requireNonNull(set);
        for (T t : set) {
            Objects.requireNonNull(t);
        }
        return set;
    }

    /**
     * Throws a {@link NullPointerException} if the {@link Map} is null or contains any null keys or null values.
     *
     * @param map The map to check.
     * @param <K> The type of the map key.
     * @param <V> The type of the map value.
     * @return The map.
     */
    public static <K, V> Map<K, V> requireNonNull(Map<K, V> map) {
        Objects.requireNonNull(map);
        for (Map.Entry<K, V> entry : map.entrySet()) {
            Objects.requireNonNull(entry.getKey());
            Objects.requireNonNull(entry.getValue());
        }
        return map;
    }

    /**
     * Adds the given mapping to the given {@link Map} if the value is not null.
     *
     * @param map The map to check.
     * @param key The key to add.
     * @param value The value to add if not null.
     * @param <K> The type of the map key.
     * @param <V> The type of the map value.
     * @return The map.
     */
    public static <K, V> Map<K, V> putNotNull(Map<K, V> map, K key, V value) {
        if (value != null) {
            map.put(key, value);
        }
        return map;
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
     * Generates an array of points from the given arguments.
     *
     * @param start The first point to begin with.
     * @param generator A function that returns the next point given the previous.
     * @param numberOfPoints The size of the resulting array.
     * @param rounding The number of maximum decimal places to round up to.
     * @return An array of points generated from the given arguments.
     */
    public static double[] generatePoints(double start, Function<Double, Double> generator, int numberOfPoints, int rounding) {
        double[] points = new double[numberOfPoints];
        double value = start;
        for (int i = 0; i < numberOfPoints; ++i) {
            points[i] = round(value, rounding);
            value = generator.apply(value);
        }
        return points;
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
        if (isNull(value)) {
            return null;
        }
        if (Type.isNumeric(value.getType())) {
            return (Number) value.getValue();
        }
        if (value.getType() == Type.BOOLEAN) {
            return (Boolean) value.getValue() ? 1L : 0L;
        }
        try {
            return (Number) value.forceCast(Type.DOUBLE).getValue();
        } catch (Exception e) {
            return null;
        }
    }

    public static boolean isNull(TypedObject typedObject) {
        return typedObject.isNull() || typedObject.getValue() == null;
    }

    /**
     * Returns if the {@link TypedObject} has type {@link TypedObject#NULL} or value null.
     *
     * @param typedObject The typed object to check for null.
     * @return true if the {@link TypedObject} has type {@link TypedObject#NULL} or value null and false otherwise.
     */
    public static boolean isNull(TypedObject typedObject) {
        return typedObject.isNull() || typedObject.getValue() == null;
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
