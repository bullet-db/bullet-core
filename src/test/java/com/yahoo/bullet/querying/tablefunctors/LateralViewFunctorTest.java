/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.tablefunctions.Explode;
import com.yahoo.bullet.query.tablefunctions.LateralView;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.record.simple.TypedSimpleBulletRecordProvider;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LateralViewFunctorTest {
    private BulletRecordProvider provider = new TypedSimpleBulletRecordProvider();
    private BulletRecord record;

    @BeforeClass
    public void setup() {
        record = RecordBox.get().addList("listA", 0, 1, 2)
                                .add("listB", new ArrayList<>())
                                .addMap("mapA", Pair.of("a", 0), Pair.of("b", 1), Pair.of("c", 2))
                                .add("mapB", new HashMap<>())
                                .getRecord();
    }

    @Test
    public void testConstructor() {
        LateralView lateralView = new LateralView(new Explode(new FieldExpression("abc"), "foo", "bar", true));
        LateralViewFunctor functor = new LateralViewFunctor(lateralView);

        Assert.assertTrue(functor.tableFunctor instanceof ExplodeFunctor);
    }

    @Test
    public void testApplyWithExplode() {
        LateralView lateralView = new LateralView(new Explode(new FieldExpression("mapA"), "foo", "bar", false));
        LateralViewFunctor functor = new LateralViewFunctor(lateralView);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 3);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(0)).getBaseRecord().fieldCount(), 4);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(0)).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(records.get(0).typedGet("foo").getValue(), "a");
        Assert.assertEquals(records.get(0).typedGet("bar").getValue(), 0);
        Assert.assertTrue(records.get(0).hasField("listA"));
        Assert.assertTrue(records.get(0).hasField("listB"));
        Assert.assertTrue(records.get(0).hasField("mapA"));
        Assert.assertTrue(records.get(0).hasField("mapB"));
        Assert.assertEquals(((LateralViewBulletRecord) records.get(1)).getBaseRecord().fieldCount(), 4);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(1)).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(records.get(1).typedGet("foo").getValue(), "b");
        Assert.assertEquals(records.get(1).typedGet("bar").getValue(), 1);
        Assert.assertTrue(records.get(1).hasField("listA"));
        Assert.assertTrue(records.get(1).hasField("listB"));
        Assert.assertTrue(records.get(1).hasField("mapA"));
        Assert.assertTrue(records.get(1).hasField("mapB"));
        Assert.assertEquals(((LateralViewBulletRecord) records.get(2)).getBaseRecord().fieldCount(), 4);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(2)).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(records.get(2).typedGet("foo").getValue(), "c");
        Assert.assertEquals(records.get(2).typedGet("bar").getValue(), 2);
        Assert.assertTrue(records.get(2).hasField("listA"));
        Assert.assertTrue(records.get(2).hasField("listB"));
        Assert.assertTrue(records.get(2).hasField("mapA"));
        Assert.assertTrue(records.get(2).hasField("mapB"));

        lateralView = new LateralView(new Explode(new FieldExpression("mapB"), "foo", "bar", false));
        functor = new LateralViewFunctor(lateralView);

        records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 0);
    }

    @Test
    public void testApplyWithOuterExplode() {
        LateralView lateralView = new LateralView(new Explode(new FieldExpression("mapB"), "foo", "bar", true));
        LateralViewFunctor functor = new LateralViewFunctor(lateralView);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 1);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(0)).getBaseRecord().fieldCount(), 4);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(0)).getTopRecord().fieldCount(), 0);
        Assert.assertTrue(records.get(0).hasField("listA"));
        Assert.assertTrue(records.get(0).hasField("listB"));
        Assert.assertTrue(records.get(0).hasField("mapA"));
        Assert.assertTrue(records.get(0).hasField("mapB"));
    }
}
