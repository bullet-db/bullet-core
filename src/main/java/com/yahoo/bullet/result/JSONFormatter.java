/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.result;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;

public interface JSONFormatter {
    JsonSerializer<Double> INVALID_DOUBLES = (item, type, context) -> item.isNaN() || item.isInfinite() ?
                                                                      new JsonPrimitive(item.toString()) : new JsonPrimitive(item);
    Gson GSON = new GsonBuilder().serializeNulls().registerTypeAdapter(Double.class, INVALID_DOUBLES).create();

    /**
     * Returns a JSON string representation of object.
     * @param object The object to make a JSON out of.
     * @return JSON string of the object.
     */
    static String asJSON(Object object) {
        return GSON.toJson(object);
    }

    /**
     * Returns a deserialized object from JSON using {@link JSONFormatter#GSON}.
     *
     * @param json The String json that represents the object.
     * @param clazz The class of the object.
     * @param <T> The type of the object. It must implement {@link JSONFormatter}.
     * @return An instance of the object deserialized from JSON.
     */
    static <T extends JSONFormatter> T fromJSON(String json, Class<T> clazz) {
        return GSON.fromJson(json, clazz);
    }

    /**
     * Convert this object to a JSON string.
     * @return The JSON representation of this.
     */
    String asJSON();
}
