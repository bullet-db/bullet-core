/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying.partitioning;

import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.record.BulletRecord;

import java.util.Set;

public interface Partitioner {
    /**
     * Returns the partitioning keys for this {@link Query} instance.
     *
     * @param query The query to partition for.
     * @return A non-null {@link Set} of Strings representing the keys for this query.
     */
    Set<String> getKeys(Query query);

    /**
     * Returns the partitioning keys for this {@link BulletRecord} instance.
     *
     * @param record The record to partition.
     * @return A non-null {@link Set} of Strings representing the keys for this record.
     */
    Set<String> getKeys(BulletRecord record);
}
