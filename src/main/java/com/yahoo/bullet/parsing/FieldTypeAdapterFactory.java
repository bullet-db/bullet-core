/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.Streams;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Adapted from * Google GSON's RuntimeTypeAdapterFactory to support a field based adapter. Instead of adding a new
 * field to the top level, this takes a list of values that the field can be in order to decide which TypeAdapter to
 * use. The field itself is represented as a passed in Function that operates on a JsonElement and returns a String.
 * You can compute your field based on the JsonElement and return a String value. Subtypes can then be registered
 * with a list of String values that this extracted field is checked against.
 *
 * When deserializing JSON, the field is extracted from it using the provided Function, checked to see if it is
 * present in the List of values for a subtype (in order of registry), if it is, the JSON is attempted to be
 * deserialized into that subtype (and returned as an element of the base type {@code T}).
 *
 * Serializing JSON is done as normal without any changes to the output JSON.
 *
 * RuntimeTypeAdapterFactory instead uses an additional field to carry the type information.
 *
 * @param <T> The base type that this factory handles.
 */
public class FieldTypeAdapterFactory<T> implements TypeAdapterFactory {
    private final Class<T> base;
    private final Map<Class<?>, Predicate<JsonObject>> registeredTypes = new LinkedHashMap<>();

    private FieldTypeAdapterFactory(Class<T> base) {
        this.base = base;
    }

    /**
     * Creates a FieldTypeAdapterFactory of this type.
     *
     * @param base The base type for all types that this factory handles.
     * @param <T> The base type.
     * @return The created factory.
     */
    public static <T> FieldTypeAdapterFactory<T> of(Class<T> base) {
        return new FieldTypeAdapterFactory<>(base);
    }

    /**
     * Register a subtype for the factory with the values it is to support.
     *
     * @param subType A subtype to handle.
     * @param condition The {@link Predicate} that will decide this class
     * @return this object for chaining.
     */
    public FieldTypeAdapterFactory<T> registerSubType(Class<? extends T> subType, Predicate<JsonObject> condition) {
        Objects.requireNonNull(subType);
        Objects.requireNonNull(condition);
        registeredTypes.put(subType, condition);
        return this;
    }

    @Override
    public <R> TypeAdapter<R> create(Gson gson, TypeToken<R> type) {
        if (type.getRawType() != base) {
            return null;
        }
        Map<Class<?>, TypeAdapter<?>> registeredAdapters = new LinkedHashMap<>();
        for (Class<?> clazz : registeredTypes.keySet()) {
            registeredAdapters.put(clazz, gson.getAdapter(clazz));
        }
        return new FieldTypeAdapter<>(registeredAdapters, registeredTypes);
    }

    // Type checking for R's super type has already happened at registration. It's safe to ignore type check warnings.
    @SuppressWarnings("unchecked")
    private static class FieldTypeAdapter<R> extends TypeAdapter<R> {
        private Map<Class<?>, TypeAdapter<?>> adapters;
        private Map<Class<?>, Predicate<JsonObject>> types;

        /**
         * Constructor for the adapter that takes an extraction mechanism and map of adapters and types.
         *
         * @param adapters A Map of Class to TypeAdapters for that Class.
         * @param types A Map of Class to the Set of Strings that are to be matched against the output of extractor.
         */
        public FieldTypeAdapter(Map<Class<?>, TypeAdapter<?>> adapters, Map<Class<?>, Predicate<JsonObject>> types) {
            this.adapters = adapters;
            this.types = types;
        }

        @Override
        public void write(JsonWriter out, R value) throws IOException {
            TypeAdapter<R> adapter = (TypeAdapter<R>) adapters.get(value.getClass());
            if (adapter == null) {
                throw new JsonParseException("Adapter not found for serializing " + value);
            }
            adapter.write(out, value);
        }

        private TypeAdapter<R> getAdapterFor(JsonElement element) {
            for (Map.Entry<Class<?>, Predicate<JsonObject>> entry : types.entrySet()) {
                if (entry.getValue().test(element.getAsJsonObject())) {
                    return (TypeAdapter<R>) adapters.get(entry.getKey());
                }
            }
            return null;
        }

        @Override
        public R read(JsonReader in) throws IOException {
            JsonElement jsonElement = Streams.parse(in);
            TypeAdapter<R> adapter = getAdapterFor(jsonElement);
            return adapter == null ? null : adapter.fromJsonTree(jsonElement);
        }
    }
}
