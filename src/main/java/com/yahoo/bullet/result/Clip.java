/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.result;

import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class Clip implements JSONFormatter {
    public static final String META_KEY = "meta";
    public static final String RECORDS_KEY = "records";

    private Meta meta = new Meta();
    private List<BulletRecord> records = new ArrayList<>();

    /**
     * Adds a {@link BulletRecord} to the records in the Clip.
     *
     * @param record The input record.
     * @return This Clip for chaining.
     */
    public Clip add(BulletRecord record) {
        if (record != null) {
            records.add(record);
        }
        return this;
    }

    /**
     * Adds all the {@link BulletRecord} to the records in the Clip.
     *
     * @param records The input records.
     * @return This Clip for chaining.
     */
    public Clip add(List<BulletRecord> records) {
        if (records != null) {
            records.forEach(this::add);
        }
        return this;
    }

    /**
     * Adds all the {@link BulletRecord} and the {@link Meta} from the given {@link Clip} to this.
     *
     * @param clip The clip to add.
     * @return This Clip for chaining.
     */
    public Clip add(Clip clip) {
        if (clip != null) {
            add(clip.getMeta());
            add(clip.getRecords());
        }
        return this;
    }

    /**
     * Tags additional metadata. Merges any new metadata with existing metadata, if present.
     *
     * @param meta Any Meta to add to the Clip. The objects in the Meta must be
     *             serializable to JSON with {@link com.google.gson.Gson}.
     * @return This Clip for chaining
     */
    public Clip add(Meta meta) {
        if (meta != null) {
            this.meta.merge(meta);
        }
        return this;
    }

    @Override
    public String asJSON() {
        Map<String, Object> wrapper = new HashMap<>();
        wrapper.put(META_KEY, meta.asMap());
        wrapper.put(RECORDS_KEY, records.stream().map(BulletRecord::toUnmodifiableDataMap).collect(Collectors.toList()));
        return JSONFormatter.asJSON(wrapper);
    }

    /**
     * Construct a Clip with the given {@link BulletRecord}.
     *
     * @param record The input record.
     * @return The created Clip.
     */
    public static Clip of(BulletRecord record) {
        return new Clip().add(record);
    }

    /**
     * Construct a Clip with the given List of {@link BulletRecord}.
     *
     * @param records The input records.
     * @return The created Clip.
     */
    public static Clip of(List<BulletRecord> records) {
        return new Clip().add(records);
    }

    /**
     * Construct a Clip with the given metadata.
     *
     * @param meta The Meta to add. The objects in the Meta must be serializable to JSON
     *             with {@link com.google.gson.Gson}.
     * @return This object for chaining
     */
    public static Clip of(Meta meta) {
        return new Clip().add(meta);
    }
}
