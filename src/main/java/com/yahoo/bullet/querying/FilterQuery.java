/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.record.BulletRecord;

public class FilterQuery extends AbstractQuery<BulletRecord, byte[]> {
    /**
     * Default constructor.
     *
     * @param input The query as a String.
     * @param config A {@link BulletConfig} configuration that has been validated.
     * @throws ParsingException if there was an issue.
     */
    public FilterQuery(String input, BulletConfig config) throws ParsingException {
        super(input, config);
    }

    /**
     * {@inheritDoc}
     *
     * Returns the raw byte[] representation of the data. This could be projected records, Sketches or GroupData.
     *
     * @return a byte[] representation of the data
     */
    @Override
    public byte[] getData() {
        return specification.getSerializedAggregate();
    }

    @Override
    public boolean consume(BulletRecord record) {
        // If query is expired, not accepting data or does not match filters, don't consume...
        if (isExpired() || !specification.isAcceptingData() || !specification.filter(record)) {
            return false;
        }
        specification.aggregate(specification.project(record));
        return specification.isMicroBatch();
    }
}
