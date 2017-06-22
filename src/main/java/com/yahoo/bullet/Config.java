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
import java.util.Optional;
import java.util.Set;

@Slf4j
public abstract class Config implements Serializable {
    private Map<String, Object> data;

    public static final String DEFAULT_CONFIGURATION_NAME = "bullet_defaults.yaml";
    private String defaultConfiguration;

    /**
     * Constructor that loads specific file augmented with defaults and the name of the default configuration file.
     *
     *
     * @param file YAML file to load.
     * @param defaultConfigurationFile Default YAML file to load.
     * @throws IOException if an error occurred with the file loading.
     */
    public Config(String file, String defaultConfigurationFile) throws IOException {
        this.defaultConfiguration = defaultConfigurationFile;
        data = loadConfigResource(file);
    }

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     * @throws IOException if an error occurred with the file loading.
     */
    public Config(String file) throws IOException {
        this(file, DEFAULT_CONFIGURATION_NAME);
    }

    /**
     * Default constructor.
     *
     * @throws IOException if an error occurred with loading the default config.
     */
    public Config() throws IOException {
        this(null);
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
     * Clears out the configuration.
     */
    public void clear() {
        data.clear();
    }

    private Map<String, Object> loadConfigResource(String yamlFile) throws IOException {
        Map<String, Object> defaultconf = readYAML(defaultConfiguration);
        Map<String, Object> specificConf = readYAML(yamlFile);
        // Override
        defaultconf.putAll(specificConf);
        log.info("Final configuration: {} ", defaultconf);
        return defaultconf;
    }

    /**
     * Reads a YAML file containing mappings and returns them as a {@link Map}.
     *
     * @param yamlFile The String name of the YAML resource file in classpath or the path to a YAML file containing the mappings.
     * @return A {@link Map} of String names to Objects of the mappings in the YAML file.
     * @throws IOException if the resource could not be read.
     */
    protected Map<String, Object> readYAML(String yamlFile) throws IOException {
        if (yamlFile != null && yamlFile.length() > 0) {
            log.info("Loading configuration file: {}", yamlFile);
            InputStream is = this.getClass().getResourceAsStream("/" + yamlFile);
            Reader reader = (is != null ? new InputStreamReader(is) : new FileReader(yamlFile));
            return (Map<String, Object>) YAML.load(reader);
        }
        return new HashMap<>();
    }
}
