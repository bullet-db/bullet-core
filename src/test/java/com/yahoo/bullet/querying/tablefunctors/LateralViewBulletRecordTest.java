/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.record.simple.UntypedSimpleBulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;
import org.junit.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Map;

public class LateralViewBulletRecordTest {
    private LateralViewBulletRecord record;

    @BeforeMethod
    public void setup() {
        record = new LateralViewBulletRecord(new UntypedSimpleBulletRecord(), new UntypedSimpleBulletRecord());
    }

    @Test
    public void testGetRawDataMap() {
        Map<String, Serializable> map = record.getRawDataMap();

        Assert.assertEquals(map.size(), 0);

        record.getBaseRecord().setString("abc", "def");
        record.getTopRecord().setString("foo", "bar");

        map = record.getRawDataMap();

        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("abc"), "def");
        Assert.assertEquals(map.get("foo"), "bar");
    }

    @Test
    public void testGetRawDataMapShadowedFieldAndCulledField() {
        record.getBaseRecord().setString("abc", "def");
        record.getTopRecord().setString("abc", "bar");

        Map<String, Serializable> map = record.getRawDataMap();

        Assert.assertEquals(map.size(), 1);
        Assert.assertEquals(map.get("abc"), "bar");

        record.remove("abc");

        map = record.getRawDataMap();

        Assert.assertEquals(map.size(), 0);
    }

    @Test
    public void testRemove() {
        record.getBaseRecord().setString("abc", "def");
        record.getTopRecord().setString("abc", "bar");

        Assert.assertTrue(record.getBaseRecord().hasField("abc"));
        Assert.assertTrue(record.getTopRecord().hasField("abc"));
        Assert.assertEquals(record.getCulledFields().size(), 0);

        // remove returns this for chaining
        Assert.assertSame(record.remove("abc"), record);

        Assert.assertTrue(record.getBaseRecord().hasField("abc"));
        Assert.assertFalse(record.getTopRecord().hasField("abc"));
        Assert.assertEquals(record.getCulledFields().size(), 1);
        Assert.assertTrue((record.getCulledFields().contains("abc")));

        // remove only affects the top record
        record.remove("abc");

        Assert.assertTrue(record.getBaseRecord().hasField("abc"));
        Assert.assertFalse(record.getTopRecord().hasField("abc"));
        Assert.assertEquals(record.getCulledFields().size(), 1);
        Assert.assertTrue((record.getCulledFields().contains("abc")));
    }

    @Test
    public void testTypedGet() {
        record.getBaseRecord().setString("abc", "def");
        record.getTopRecord().setString("foo", "bar");

        Assert.assertEquals(record.typedGet("abc").getValue(), "def");
        Assert.assertEquals(record.typedGet("foo").getValue(), "bar");
        Assert.assertNull(record.typedGet("123").getValue());
    }

    @Test
    public void testTypedGetShadowedFieldAndCulledField() {
        record.getBaseRecord().setString("abc", "def");
        record.getTopRecord().setString("abc", "bar");

        Assert.assertEquals(record.typedGet("abc").getValue(), "bar");

        record.remove("abc");

        Assert.assertNull(record.typedGet("abc").getValue());
    }

    @Test
    public void testTypedSet() {
        record.getBaseRecord().setString("abc", "def");

        Assert.assertTrue(record.getBaseRecord().hasField("abc"));
        Assert.assertFalse(record.getTopRecord().hasField("abc"));

        record.typedSet("abc", TypedObject.valueOf("def"));

        Assert.assertTrue(record.getBaseRecord().hasField("abc"));
        Assert.assertTrue(record.getTopRecord().hasField("abc"));
    }

    @Test
    public void testTypedGetAndTypedSetCulledField() {
        record.getBaseRecord().setString("abc", "def");
        record.getTopRecord().setString("abc", "bar");
        record.remove("abc");

        Assert.assertNull(record.typedGet("abc").getValue());
        Assert.assertEquals(record.getCulledFields().size(), 1);
        Assert.assertTrue(record.getCulledFields().contains("abc"));

        record.typedSet("abc", TypedObject.valueOf("bar"));

        Assert.assertEquals(record.typedGet("abc").getValue(), "bar");
        Assert.assertEquals(record.getCulledFields().size(), 0);
    }

    @Test
    public void testCopy() {
        Assert.assertSame(record, record.copy());
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testConvert() {
        record.convert(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testRawSet() {
        record.rawSet(null, null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGet() {
        record.get(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testHasField() {
        record.hasField(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testFieldCount() {
        record.fieldCount();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetAndRemove() {
        record.getAndRemove(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testIterator() {
        record.iterator();
    }
}
