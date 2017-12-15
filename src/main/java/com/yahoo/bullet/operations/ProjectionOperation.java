package com.yahoo.bullet.operations;

import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.record.BulletRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.yahoo.bullet.common.Utilities.splitField;

@Slf4j
public class ProjectionOperation {
    /**
     * Projects the given {@link BulletRecord} based on the given {@link Projection}.
     *
     * @param record The record to project.
     * @param projection The projection to apply.
     * @return The projected record.
     */
    public static BulletRecord project(BulletRecord record, Projection projection) {
        Map<String, String> fields = projection.getFields();
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
