/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.tablefunctions.Explode;
import com.yahoo.bullet.query.tablefunctions.LateralView;
import com.yahoo.bullet.query.tablefunctions.TableFunction;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.record.simple.TypedSimpleBulletRecordProvider;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LateralViewFunctorTest {
    private BulletRecordProvider provider = new TypedSimpleBulletRecordProvider();
    private BulletRecord record;
    private Map<String, Serializable> mapD = new HashMap<>();
    private Map<String, Serializable> mapE = new HashMap<>();

    @BeforeClass
    public void setup() {
        mapD.put("d", 3);
        mapD.put("e", 4);
        mapE.put("f", 5);
        mapE.put("g", 6);
        record = RecordBox.get().addList("listA", 0, 1, 2)
                                .add("listB", new ArrayList<>())
                                .addMap("mapA", Pair.of("a", 0), Pair.of("b", 1), Pair.of("c", 2))
                                .add("mapB", new HashMap<>())
                                .addMapOfMaps("mapC", Pair.of("mapD", mapD), Pair.of("mapE", mapE))
                                .getRecord();
    }

    @Test
    public void testConstructor() {
        LateralView lateralView = new LateralView(new Explode(new FieldExpression("abc"), "foo", "bar", true));
        LateralViewFunctor functor = new LateralViewFunctor(lateralView);

        Assert.assertTrue(functor.tableFunctors.get(0) instanceof ExplodeFunctor);
    }

    @Test
    public void testApplyWithExplode() {
        LateralView lateralView = new LateralView(new Explode(new FieldExpression("mapA"), "foo", "bar", false));
        LateralViewFunctor functor = new LateralViewFunctor(lateralView);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 3);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(0)).getBaseRecord().fieldCount(), 5);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(0)).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(records.get(0).typedGet("foo").getValue(), "a");
        Assert.assertEquals(records.get(0).typedGet("bar").getValue(), 0);
        Assert.assertFalse(records.get(0).typedGet("listA").isNull());
        Assert.assertFalse(records.get(0).typedGet("listB").isNull());
        Assert.assertFalse(records.get(0).typedGet("mapA").isNull());
        Assert.assertFalse(records.get(0).typedGet("mapB").isNull());
        Assert.assertEquals(((LateralViewBulletRecord) records.get(1)).getBaseRecord().fieldCount(), 5);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(1)).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(records.get(1).typedGet("foo").getValue(), "b");
        Assert.assertEquals(records.get(1).typedGet("bar").getValue(), 1);
        Assert.assertFalse(records.get(1).typedGet("listA").isNull());
        Assert.assertFalse(records.get(1).typedGet("listB").isNull());
        Assert.assertFalse(records.get(1).typedGet("mapA").isNull());
        Assert.assertFalse(records.get(1).typedGet("mapB").isNull());
        Assert.assertEquals(((LateralViewBulletRecord) records.get(2)).getBaseRecord().fieldCount(), 5);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(2)).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(records.get(2).typedGet("foo").getValue(), "c");
        Assert.assertEquals(records.get(2).typedGet("bar").getValue(), 2);
        Assert.assertFalse(records.get(2).typedGet("listA").isNull());
        Assert.assertFalse(records.get(2).typedGet("listB").isNull());
        Assert.assertFalse(records.get(2).typedGet("mapA").isNull());
        Assert.assertFalse(records.get(2).typedGet("mapB").isNull());

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
        Assert.assertEquals(((LateralViewBulletRecord) records.get(0)).getBaseRecord().fieldCount(), 5);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(0)).getTopRecord().fieldCount(), 0);
        Assert.assertFalse(records.get(0).typedGet("listA").isNull());
        Assert.assertFalse(records.get(0).typedGet("listB").isNull());
        Assert.assertFalse(records.get(0).typedGet("mapA").isNull());
        Assert.assertFalse(records.get(0).typedGet("mapB").isNull());
    }

    @Test
    public void testApplyWithMultipleExplode() {
        List<TableFunction> tableFunctions = Arrays.asList(new Explode(new FieldExpression("mapC"), "foo", "bar", false),
                                                           new Explode(new FieldExpression("bar"), "key", "value", false));
        LateralView lateralView = new LateralView(tableFunctions);
        LateralViewFunctor functor = new LateralViewFunctor(lateralView);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 4);
        Assert.assertEquals(((LateralViewBulletRecord) ((LateralViewBulletRecord) records.get(0)).getBaseRecord()).getBaseRecord().fieldCount(), 5);
        Assert.assertEquals(((LateralViewBulletRecord) ((LateralViewBulletRecord) records.get(0)).getBaseRecord()).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(0)).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(((LateralViewBulletRecord) ((LateralViewBulletRecord) records.get(1)).getBaseRecord()).getBaseRecord().fieldCount(), 5);
        Assert.assertEquals(((LateralViewBulletRecord) ((LateralViewBulletRecord) records.get(1)).getBaseRecord()).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(1)).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(((LateralViewBulletRecord) ((LateralViewBulletRecord) records.get(2)).getBaseRecord()).getBaseRecord().fieldCount(), 5);
        Assert.assertEquals(((LateralViewBulletRecord) ((LateralViewBulletRecord) records.get(2)).getBaseRecord()).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(2)).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(((LateralViewBulletRecord) ((LateralViewBulletRecord) records.get(3)).getBaseRecord()).getBaseRecord().fieldCount(), 5);
        Assert.assertEquals(((LateralViewBulletRecord) ((LateralViewBulletRecord) records.get(3)).getBaseRecord()).getTopRecord().fieldCount(), 2);
        Assert.assertEquals(((LateralViewBulletRecord) records.get(3)).getTopRecord().fieldCount(), 2);

        Assert.assertTrue(records.stream().anyMatch(r -> r.typedGet("foo").getValue().equals("mapD") &&
                                                         r.typedGet("bar").getValue().equals(mapD) &&
                                                         r.typedGet("key").getValue().equals("d") &&
                                                         r.typedGet("value").getValue().equals(3)));

        Assert.assertTrue(records.stream().anyMatch(r -> r.typedGet("foo").getValue().equals("mapD") &&
                                                         r.typedGet("bar").getValue().equals(mapD) &&
                                                         r.typedGet("key").getValue().equals("e") &&
                                                         r.typedGet("value").getValue().equals(4)));

        Assert.assertTrue(records.stream().anyMatch(r -> r.typedGet("foo").getValue().equals("mapE") &&
                                                         r.typedGet("bar").getValue().equals(mapE) &&
                                                         r.typedGet("key").getValue().equals("f") &&
                                                         r.typedGet("value").getValue().equals(5)));

        Assert.assertTrue(records.stream().anyMatch(r -> r.typedGet("foo").getValue().equals("mapE") &&
                                                         r.typedGet("bar").getValue().equals(mapE) &&
                                                         r.typedGet("key").getValue().equals("g") &&
                                                         r.typedGet("value").getValue().equals(6)));
    }
}
