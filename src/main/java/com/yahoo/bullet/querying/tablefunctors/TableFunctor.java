/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table functors are built from table functions. A table functor is applied to a {@link BulletRecord} and generates a
 * list of {@link BulletRecord}.
 *
 * Classes inheriting from this class must implement {@link TableFunctor#apply(BulletRecord, BulletRecordProvider)} but
 * not the lateral view and outer options which are already implemented in {@link TableFunctor#getRecords(BulletRecord,
 * BulletRecordProvider)}.
 */
@AllArgsConstructor
public abstract class TableFunctor implements Serializable {
    private static final long serialVersionUID = 5843896863161739195L;

    protected final boolean lateralView;
    protected final boolean outer;

    /**
     * Applies this table functor to the given {@link BulletRecord} with the lateral view and/or outer options if
     * specified.
     *
     * @param record The Bullet record to apply this table functor to.
     * @param provider The provider used in generating new Bullet records.
     * @return The generated list of records from applying this table functor to the given Bullet record.
     */
    public List<BulletRecord> getRecords(BulletRecord record, BulletRecordProvider provider) {
        List<BulletRecord> records = apply(record, provider);
        if (outer && records.isEmpty()) {
            records = Collections.singletonList(provider.getInstance());
        }
        if (lateralView) {
            return records.stream().map(generated -> {
                BulletRecord lateralViewRecord = record.copy();
                for (Map.Entry<String, ? extends Serializable> entry : ((BulletRecord<? extends Serializable>) generated)) {
                    lateralViewRecord.set(entry.getKey(), generated, entry.getKey());
                }
                return lateralViewRecord;
            }).collect(Collectors.toList());
        } else {
            return records;
        }
    }

    /**
     * Applies this table functor to the given {@link BulletRecord}.
     *
     * @param record The Bullet record to apply this table functor to.
     * @param provider The provider used in generating new Bullet records.
     * @return The generated list of records from applying this table functor to the given Bullet record.
     */
    protected abstract List<BulletRecord> apply(BulletRecord record, BulletRecordProvider provider);
}
