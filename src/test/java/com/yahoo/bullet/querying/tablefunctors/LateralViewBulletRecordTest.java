/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.record.simple.UntypedSimpleBulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Map;

public class LateralViewBulletRecordTest {
    private LateralViewBulletRecord record = new LateralViewBulletRecord(new UntypedSimpleBulletRecord(), new UntypedSimpleBulletRecord());

    @Test
    public void testGetRawDataMap() {
        record.getBaseRecord().setString("abc", "def");
        record.getTopRecord().setString("abc", "ghi");

        Map<String, Serializable> map = record.getRawDataMap();

        Assert.assertEquals(map.size(), 1);
        Assert.assertEquals(map.get("abc"), "ghi");

        // "abc" removed from the top record and also added to culled but still exists in base record
        record.remove("abc");

        Assert.assertEquals(record.getCulledFields().size(), 1);
        Assert.assertTrue(record.getCulledFields().contains("abc"));
        Assert.assertEquals(record.typedGet("abc").getValue(), "def");

        map = record.getRawDataMap();

        Assert.assertEquals(map.size(), 0);

        // "abc" set in top record and also removed from culled. New "abc" shadows the field in base record
        record.typedSet("abc", TypedObject.valueOf("123"));

        Assert.assertEquals(record.getCulledFields().size(), 0);
        Assert.assertEquals(record.typedGet("abc").getValue(), "123");

        map = record.getRawDataMap();

        Assert.assertEquals(map.size(), 1);
        Assert.assertEquals(map.get("abc"), "123");
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

    @Test
    public void testCopy() {
        Assert.assertSame(record, record.copy());
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testIterator() {
        record.iterator();
    }
}
