/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.operations;

import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ProjectionOperations {
    /**
     * Projects the given {@link BulletRecord} based on the given fields.
     *
     * @param record The record to project.
     * @param projection The projection to apply.
     * @param transientFields The map of fields to apply.
     * @param provider A BulletRecordProvider to generate BulletRecords.
     * @return The projected record.
     */
    public static BulletRecord project(BulletRecord record, Projection projection, Map<String, String> transientFields, BulletRecordProvider provider) {
        /*
        Map<String, String> fields = new HashMap<>();
        Map<String, String> projectionFields = projection.getFields();
        if (projectionFields != null) {
            fields.putAll(projectionFields);
        }
        if (transientFields != null) {
            fields.putAll(transientFields);
        }
        // Returning the record itself if no projection. The record itself should never be modified so it's ok.
        if (fields.isEmpty()) {
            return record;
        }
        // More efficient if fields << the fields in the BulletRecord
        BulletRecord projected = provider.getInstance();
        for (Map.Entry<String, String> e : fields.entrySet()) {
            String field = e.getKey();
            String newName = e.getValue();
            if (field != null) {
                projected.forceSet(newName, record.extractField(field));
            }
        }
        return projected;
        */
        return null;
    }
}
