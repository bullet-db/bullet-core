/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This BulletRecord is only used for Lateral View and possible operations in the Querier. It is not meant to be a
 * fully-functional BulletRecord.
 */
@RequiredArgsConstructor
@Getter(AccessLevel.PACKAGE)
class LateralViewBulletRecord extends BulletRecord {
    private static final long serialVersionUID = 1756000447237973392L;

    private final BulletRecord baseRecord;
    private final BulletRecord topRecord;
    private Set<String> culledFields = new HashSet<>();

    @Override
    protected Serializable convert(Object object) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BulletRecord rawSet(String field, Serializable object) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Map<String, Serializable> getRawDataMap() {
        Map<String, Serializable> map = new HashMap<>(baseRecord.toUnmodifiableDataMap());
        map.putAll(topRecord.toUnmodifiableDataMap());
        map.keySet().removeAll(culledFields);
        return map;
    }

    @Override
    public Serializable get(String field) {
        return topRecord.hasField(field) ? topRecord.get(field) : baseRecord.get(field);
    }

    @Override
    public boolean hasField(String field) {
        return topRecord.hasField(field) || baseRecord.hasField(field);
    }

    @Override
    public int fieldCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Serializable getAndRemove(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulletRecord remove(String field) {
        culledFields.add(field);
        return topRecord.remove(field);
    }

    @Override
    public TypedObject typedGet(String field) {
        return topRecord.hasField(field) ? topRecord.typedGet(field) : baseRecord.typedGet(field);
    }

    @Override
    public BulletRecord typedSet(String field, TypedObject object) {
        culledFields.remove(field);
        topRecord.typedSet(field, object);
        return this;
    }

    @Override
    public BulletRecord copy() {
        // LateralViewBulletRecord does not need to be copied in the Querier Projection since the topRecord is one-use anyways.
        return this;
    }

    @Override
    public Iterator iterator() {
        throw new UnsupportedOperationException();
    }
}
