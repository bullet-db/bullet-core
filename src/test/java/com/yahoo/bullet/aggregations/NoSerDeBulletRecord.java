/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.record.BulletRecord;

import java.io.IOException;
import java.io.Serializable;

public class NoSerDeBulletRecord extends BulletRecord implements Serializable {
    private static final long serialVersionUID = 4138653240854288567L;

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        throw new IOException("Forced test serialization failure");
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        throw new IOException("Forced test deserialization failure");
    }
}
