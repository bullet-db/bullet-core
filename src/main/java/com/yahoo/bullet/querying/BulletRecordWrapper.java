/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@RequiredArgsConstructor
public class BulletRecordWrapper extends BulletRecord {
    private static final long serialVersionUID = 2194169401061695144L;

    private final BulletRecord baseRecord;
    private Map<String, TypedObject> map = new HashMap<>();

    @Override
    public TypedObject typedGet(String field) {
        TypedObject value = map.get(field);
        if (value != null) {
            return value;
        }
        return baseRecord.typedGet(field);
    }

    public void put(String field, Serializable value) {
        map.put(field, new TypedObject(value));
    }

    @Override
    protected Serializable convert(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BulletRecord rawSet(String s, Serializable serializable) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Map<String, Serializable> getRawDataMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Serializable get(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasField(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int fieldCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Serializable getAndRemove(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulletRecord remove(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulletRecord copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator iterator() {
        throw new UnsupportedOperationException();
    }
}
