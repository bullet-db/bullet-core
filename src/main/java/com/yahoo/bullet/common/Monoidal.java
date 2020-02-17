/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;

import java.util.List;

/**
 * This interface captures the associative operations that can be performed within Bullet. The identity is this object
 * when initially constructed or when {@link #reset()}. This is a special case of a Monoid that works on
 * {@link BulletRecord} and allows you to collapse chained combinations of them with {@link #combine(byte[])} or
 * {@link #merge(Monoidal)}. You can extract the data using {@link #getData()} to use with {@link #combine(byte[])} or
 * as results to present to the work with {@link #getResult()} or {@link #getRecords()} or {@link #getMetadata()}. Note
 * that {@link #getMetadata()} might not be strictly monoidal since they may not reset to the identity upon {@link #reset()}.
 */
public interface Monoidal extends Initializable, Closable {
    /**
     * Consumes a single {@link BulletRecord} into this Monoid.
     *
     * @param data The {@link BulletRecord} to consume.
     */
    void consume(BulletRecord data);

    /**
     * Combines a serialized piece of data into this Monoid.
     *
     * @param data A serialized representation of the data produced by {@link #getData()}.
     */
    void combine(byte[] data);

    /**
     * Combine the data from another instance of the same type of object into this Monoid. By default, just
     * unconditionally calls the {@link Monoidal#getData()} method on the object and passes it to
     * {@link #combine(byte[])}.
     *
     * @param other The non-null other object to combine data from.
     */
    default void merge(Monoidal other) {
        combine(other.getData());
    }

    /**
     * Get the data serialized as a byte[].
     *
     * @return the serialized representation of the data so far.
     */
    byte[] getData();

    /**
     * Get the data so far as a {@link Clip}.
     *
     * @return The resulting {@link Clip} representing the data and the metadata so far.
     */
    Clip getResult();

    /**
     * Get the data so far as a {@link List} of {@link BulletRecord}.
     *
     * @return The resulting list of records representing the data.
     */
    List<BulletRecord> getRecords();

    /**
     * Get the metadata only of the data so far. Might not be {@link #reset()}.
     *
     * @return The {@link Meta} collected so far.
     */
    Meta getMetadata();

    /**
     * Reset the data so far and make this the identity element for the Monoid with respect to inserting data.
     */
    void reset();
}
