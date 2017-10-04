/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import lombok.extern.slf4j.Slf4j;
import org.jvyaml.YAML;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class Config implements Serializable {
    private Map<String, Object> data;
    public static final String DELIMITER = ".";

    /**
     * Constructor that loads a specific file and loads the settings in that file.
     *
     * @param file YAML file to load.
     */
    public Config(String file) {
        data = readYAML(file);
        log.info("Final Configuration:\n{} ", data);
    }

    /**
     * Constructor that loads specific file augmented with defaults and the name of the default configuration file.
     *
     * @param file YAML file to load.
     * @param defaultConfigurationFile Default YAML file to load.
     */
    public Config(String file, String defaultConfigurationFile) {
        data = readYAML(defaultConfigurationFile);
        // Override
        Map<String, Object> specificConf = readYAML(file);
        data.putAll(specificConf);
        log.info("Final Configuration with defaults:\n{} ", data);
    }

    /**
     * Looks up the key and returns the matching value or null if not.
     *
     * @param key The key to use
     * @return value that the key maps to or null.
     */
    public Object get(String key) {
        return data.get(key);
    }

    /**
     * If there was a mapping for the key, the mapping is returned if not null, otherwise returns the defaultValue.
     *
     * @param key The key to get.
     * @param defaultValue The defaultValue to return if there is no mapping or the mapping is null.
     * @return The value of the key or the defaultValue.
     */
    public Object getOrDefault(String key, Object defaultValue) {
        Object value = get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Get a value from the config as a particular type.
     *
     * @param key The name of the config.
     * @param clazz The Class of the type.
     * @param <T> The type of the config.
     * @return The config as the particular type or null.
     * @throws ClassCastException if the value of the config could not be casted to the type.
     */
    public <T> T getAs(String key, Class<T> clazz) {
        return clazz.cast(get(key));
    }

    /**
     * Get a value from the config as a particular type or default to a provided value.
     *
     * @param key The name of the config.
     * @param defaultValue A default of the same type to use if the config was not found.
     * @param clazz The Class of the type.
     * @param <T> The type of the config.
     * @return The config or your default value as the particular type.
     * @throws ClassCastException if the value of the config or default could not be casted to the type.
     */
    public <T> T getOrDefaultAs(String key, T defaultValue, Class<T> clazz) {
        return clazz.cast(getOrDefault(key, defaultValue));
    }

    /**
     * Get a value from the config as a particular type or throw an exception with a message if not found..
     *
     * @param key The name of the config.
     * @param clazz The Class of the type.
     * @param <T> The type of the config.
     * @return The config as the particular type.
     * @throws ClassCastException if the value of the config could not be casted to the type.
     * @throws NullPointerException if the config was not found.
     */
    public <T> T getRequiredConfigAs(String key, Class<T> clazz) {
        return Objects.requireNonNull(getAs(key, clazz), "Required value for " + key + " was missing");
    }

    /**
     * Gets all mappings for a set of keys. If no keys are specified, all mappings
     * are returned.
     *
     * @param keys an {@link Optional} {@link Set} of mapping names.
     * @return a mapping (non-backing) of keys if present, or all the mappings.
     */
    public Map<String, Object> getAll(Optional<Set<String>> keys) {
        Set<String> inclusions = keys.orElse(data.keySet());
        return this.data.entrySet().stream()
                        .filter(e -> inclusions.contains(e.getKey()))
                        .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);
    }

    /**
     * Get mappings for all keys with the specified prefix. If stripPrefix is set, the output keys do not contain the
     * prefix.
     *
     * @param keys an {@link Optional} {@link Set} of mapping names.
     * @param prefix The prefix that relevant keys must contain.
     * @param stripPrefix If true, the output keys do not contain the prefix.
     * @return mapping for keys (or all keys in data, if keys is empty) with the prefix.
     */
    public Map<String, Object> getAllWithPrefix(Optional<Set<String>> keys, String prefix, boolean stripPrefix) {
        Set<String> inclusions = keys.orElse(data.keySet());
        int prefixLength = stripPrefix ? prefix.length() : 0;
        return this.data.entrySet().stream()
                        .filter(e -> inclusions.contains(e.getKey()))
                        .filter(e -> e.getKey().startsWith(prefix))
                        .collect(HashMap::new, (m, e) -> m.put(e.getKey().substring(prefixLength), e.getValue()), HashMap::putAll);
    }

    /**
     * Gets all mappings other than a set of keys. If no keys are specified, all mappings
     * are returned.
     *
     * @param keys an {@link Optional} {@link Set} of mapping names.
     * @return a mapping (non-backing) of keys if present, or all the mappings.
     */
    public Map<String, Object> getAllBut(Optional<Set<String>> keys) {
        Set<String> exclusions = keys.orElse(new HashSet<>());
        return this.data.entrySet().stream()
                        .filter(e -> !exclusions.contains(e.getKey()))
                        .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);
    }

    /**
     * Adds a key/value pair to the configuration.
     *
     * @param key to use
     * @param value to use
     */
    public void set(String key, Object value) {
        data.put(key, value);
    }

    /**
     * Merges another Config into this one.
     *
     * @param other The other {@link Config} to merge into this one.
     */
    public void merge(Config other) {
        if (other != null) {
            data.putAll(other.data);
        }
    }

    /**
     * Clears out the configuration.
     */
    public void clear() {
        data.clear();
    }

    /**
     * Reads a YAML file containing mappings and returns them as a {@link Map}.
     *
     * @param yamlFile The String name of the YAML resource file in classpath or the path to a YAML file containing the mappings.
     * @return A {@link Map} of String names to Objects of the mappings in the YAML file.
     */
    protected Map<String, Object> readYAML(String yamlFile) {
        if (yamlFile == null || yamlFile.isEmpty()) {
            return new HashMap<>();
        }
        log.info("Loading configuration file: {}", yamlFile);
        try {
            InputStream is = this.getClass().getResourceAsStream("/" + yamlFile);
            Reader reader = (is != null ? new InputStreamReader(is) : new FileReader(yamlFile));
            return (Map<String, Object>) YAML.load(reader);
        } catch (IOException ioe) {
            log.error("Error loading configuration", ioe);
            return new HashMap<>();
        }
    }
}
