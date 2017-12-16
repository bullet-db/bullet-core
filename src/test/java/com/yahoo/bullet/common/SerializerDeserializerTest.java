/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.common.SerializerDeserializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SerializerDeserializerTest {
    private static class SampleObject implements Serializable {
        public static final long serialVersionUID = 387461949277948303L;

        public Map<String, Double> contents = new HashMap<>();
    }

    private static class UnserializableObject {
        private int foo = 0;
    }

    private static class UnserializableSampleObject extends SampleObject {
        private UnserializableObject object = new UnserializableObject();
    }

    private SampleObject make(byte[] data) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (SampleObject) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] unmake(SampleObject data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream ois = new ObjectOutputStream(bos);
            ois.writeObject(data);
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testSerializationFailing() {
        UnserializableSampleObject bad = new UnserializableSampleObject();
        Assert.assertNull(SerializerDeserializer.toBytes(bad));
    }

    @Test
    public void testSerialization() {
        SampleObject data = new SampleObject();
        data.contents.put("foo", 42.0);

        byte[] serialized = SerializerDeserializer.toBytes(data);
        Assert.assertNotNull(serialized);

        SampleObject remade = make(serialized);
        Assert.assertEquals(remade.contents, Collections.singletonMap("foo", 42.0));
    }

    @Test
    public void testDeserializationFailing() {
        Assert.assertNull(SerializerDeserializer.fromBytes(null));
    }

    @Test
    public void testDeserialization() {
        SampleObject data = new SampleObject();
        data.contents.put("foo", 42.0);
        data.contents.put("bar", 84.0);

        byte[] serialized = unmake(data);
        SampleObject remade = SerializerDeserializer.fromBytes(serialized);

        Assert.assertNotNull(remade);
        Assert.assertEquals(remade.contents.size(), 2);
        Assert.assertEquals(remade.contents.get("foo"), 42.0);
        Assert.assertEquals(remade.contents.get("bar"), 84.0);
    }
}
