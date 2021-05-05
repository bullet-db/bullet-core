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
import java.util.List;

/**
 * Table functors are built from table functions. A table functor is applied to a {@link BulletRecord} and generates a
 * list of {@link BulletRecord}.
 *
 * Classes inheriting from this class must implement {@link TableFunctor#apply(BulletRecord, BulletRecordProvider)}.
 */
@AllArgsConstructor
public abstract class TableFunctor implements Serializable {
    private static final long serialVersionUID = 5843896863161739195L;

    /**
     * Applies this table functor to the given {@link BulletRecord}.
     *
     * @param record The Bullet record to apply this table functor to.
     * @param provider The provider used in generating new Bullet records.
     * @return The generated list of records from applying this table functor to the given Bullet record.
     */
    public abstract List<BulletRecord> apply(BulletRecord record, BulletRecordProvider provider);
}
