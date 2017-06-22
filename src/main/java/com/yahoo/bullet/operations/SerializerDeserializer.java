/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@Slf4j
public class SerializerDeserializer {
    /**
     * Convenience method to deserialize an object into a type from raw serialized data produced by the method
     * {@link #toBytes(Serializable)}.
     *
     * @param data The raw serialized byte[] representing the data.
     * @param <U> The class to try to get the data as.
     * @return A reified object or null if not successful.
     */
    public static <U extends Serializable> U fromBytes(byte[] data) {
        try (
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bis)
        ) {
            return (U) ois.readObject();
        } catch (IOException | ClassNotFoundException | RuntimeException e) {
            log.error("Could not reify an Object from raw data {}", data);
            log.error("Exception was: ", e);
        }
        return null;
    }

    /**
     * Convenience method to serializes the given {@link Serializable} object to a raw byte[].
     *
     * @param object The object to serialize.
     * @param <U> The subtype of {@link Serializable} to try and serialize.
     * @return the serialized byte[] or null if not successful.
     */
    public static <U extends Serializable> byte[] toBytes(U object) {
        try (
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos)
        ) {
            oos.writeObject(object);
            return bos.toByteArray();
        } catch (IOException | RuntimeException e) {
            log.error("Could not serialize given object", object);
            log.error("Exception was: ", e);
        }
        return null;
    }
}
