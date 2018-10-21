/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.partitioning;

import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.record.BulletRecord;

public interface Partitioner {
    /**
     * Returns the partitioning key for this {@link Query} instance.
     *
     * @param query The query to partition for.
     * @return A String representing the key for this query.
     */
    String getKey(Query query);

    /**
     * Returns the partitioning key for this {@link BulletRecord} instance.
     *
     * @param record The record to partition.
     * @return A String representing the key for this record.
     */
    String getKey(BulletRecord record);
}
