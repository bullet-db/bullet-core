package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.BulletConfig;

import java.io.IOException;

public class PubSubConfig extends BulletConfig {
    public static final String CONTEXT_NAME = "bullet.pubsub.context.name";
    public static final String PUBSUB_CLASS_NAME = "bullet.pubsub.class.name";

    /**
     * Constructor that loads configuration parameters from a specific file augmented by defaults.
     *
     * @param file YAML file to load.
     * @throws IOException if an error occurs when loading the file.
     */
    public PubSubConfig(String file) throws IOException {
        // Load settings and merge with bullet defaults.
        super(file);
    }
}
