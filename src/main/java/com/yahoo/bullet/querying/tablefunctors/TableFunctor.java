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

@AllArgsConstructor
public abstract class TableFunctor implements Serializable {
    private static final long serialVersionUID = 5843896863161739195L;

    // If true, the function returns null if the input is empty or null. If false, the function returns nothing.
    protected final boolean outer;

    public abstract List<BulletRecord> apply(BulletRecord record, BulletRecordProvider provider);
}
