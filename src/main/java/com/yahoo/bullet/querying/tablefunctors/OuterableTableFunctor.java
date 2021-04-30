/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;

/**
 * The OuterableTableFunctor class adds an outer option. When outer is specified and no records are generated, an empty
 * record will be generated.
 *
 * Classes inheriting from this class must implement {@link OuterableTableFunctor#outerableApply(BulletRecord, BulletRecordProvider)}
 * and not {@link TableFunctor#apply(BulletRecord, BulletRecordProvider)}.
 */
@AllArgsConstructor
public abstract class OuterableTableFunctor extends TableFunctor {
    private static final long serialVersionUID = 5523194920317863876L;

    protected final boolean outer;

    @Override
    public List<BulletRecord> apply(BulletRecord record, BulletRecordProvider provider) {
        List<BulletRecord> records = outerableApply(record, provider);
        if (records.isEmpty() && outer) {
            return Collections.singletonList(provider.getInstance());
        }
        return records;
    }

    /**
     * Applies this table functor to the given {@link BulletRecord}. This method is called by the main
     * {@link #apply(BulletRecord, BulletRecordProvider)} method which will handle the outer option.
     *
     * @param record The Bullet record to apply this table functor to.
     * @param provider The provider used in generating new Bullet records.
     * @return The generated list of records from applying this table functor to the given Bullet record.
     */
    protected abstract List<BulletRecord> outerableApply(BulletRecord record, BulletRecordProvider provider);
}
