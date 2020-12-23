/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class StorageConfig extends BulletConfig {
    private static final long serialVersionUID = 2635594185278740577L;

    // Field names
    public static final String PREFIX = "bullet.storage.";
    public static final String NAMESPACES = PREFIX + "namespaces";
    public static final String PARTITION_COUNT = PREFIX + "partition.count";

    // Defaults
    public static final int DEFAULT_PARTITION_COUNT = 1;
    public static final String DEFAULT_NAMESPACE = "";
    public static final List<String> DEFAULT_NAMESPACES = Collections.singletonList(DEFAULT_NAMESPACE);

    private static final Validator VALIDATOR = new Validator();
    static {
        VALIDATOR.define(NAMESPACES)
                 .defaultTo(DEFAULT_NAMESPACES)
                 .checkIf(Validator.isListOfType(String.class))
                 .checkIf(Validator::isNonEmptyList)
                 .castTo(StorageConfig::asSet);
        VALIDATOR.define(PARTITION_COUNT)
                 .defaultTo(DEFAULT_PARTITION_COUNT)
                 .checkIf(Validator::isPositiveInt)
                 .unless(Validator::isNull)
                 .castTo(Validator::asInt);
    }

    /**
     * Constructor that loads specific file augmented with defaults.
     *
     * @param file YAML file to load.
     */
    public StorageConfig(String file) {
        this(new BulletConfig(file));
    }

    /**
     * Constructor that loads the defaults and augments it with defaults.
     *
     * @param other The other config to wrap.
     */
    public StorageConfig(Config other) {
        super();
        merge(other);
        log.info("Merged settings:\n {}", this);
    }

    @Override
    public StorageConfig validate() {
        super.validate();
        VALIDATOR.validate(this);
        return this;
    }

    @SuppressWarnings("unchecked")
    private static Object asSet(Object o) {
        List<String> asList = (List<String>) o;
        return new HashSet<>(asList);
    }
}
