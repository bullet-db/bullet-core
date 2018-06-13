/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.yahoo.bullet.common.Utilities.splitField;

@Slf4j
public class ProjectionOperations {
    /**
     * Projects the given {@link BulletRecord} based on the given {@link Projection}.
     *
     * @param record The record to project.
     * @param projection The projection to apply.
     * @param bulletRecordProvider A BulletRecordProvider to generate BulletRecords.
     * @return The projected record.
     */
    public static BulletRecord project(BulletRecord record, Projection projection, BulletRecordProvider bulletRecordProvider) {
        Map<String, String> fields = projection.getFields();
        // Returning the record itself if no projections. The record itself should never be modified so it's ok.
        if (fields == null) {
            return record;
        }
        // More efficient if fields << the fields in the BulletRecord
        BulletRecord projected = bulletRecordProvider.getInstance();
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

    private static void copyInto(BulletRecord record, String newName, BulletRecord source, String field) throws ClassCastException {
        if (field == null) {
            return;
        }
        String[] split = splitField(field);
        if (split.length > 1) {
            record.set(newName, source, split[0], split[1]);
        } else {
            record.set(newName, source, field);
        }
    }
}
