/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j @Getter @Setter
public class Projection implements Configurable, Validatable {
    /**
     * The map of source field names to their new projected names.
     */
    @Expose
    private Map<String, String> fields;

    /**
     * Default constructor. GSON recommended.
     */
    public Projection() {
        fields = null;
    }

    /**
     * Applies the projection.
     * @param record The record to project from.
     * @return The projected record.
     */
    public BulletRecord project(BulletRecord record) {
        // Returning the record itself if no projections. The record itself should never be modified so it's ok.
        if (fields == null) {
            return record;
        }
        // More efficient if fields << the fields in the BulletRecord
        BulletRecord projected = new BulletRecord();
        for (Map.Entry<String, String> e : fields.entrySet()) {
            String field = e.getKey();
            String newName = e.getValue();
            try {
                copyInto(projected, newName, record, field);
            } catch (ClassCastException cce) {
                log.warn("Skipping copying {} as {} as it is not a field that can be extracted", field, newName);
            }
        }
        return projected;
    }

    private void copyInto(BulletRecord record, String newName, BulletRecord source, String field) throws ClassCastException {
        if (field == null) {
            return;
        }
        String[] split = Specification.getFields(field);
        if (split.length > 1) {
            record.set(newName, source, split[0], split[1]);
        } else {
            record.set(newName, source, field);
        }
    }

    @Override
    public Optional<List<Error>> validate() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "{fields: " + fields + "}";
    }
}
