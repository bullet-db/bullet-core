/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.query.tablefunctions.Explode;
import com.yahoo.bullet.querying.evaluators.FieldEvaluator;
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

public class ExplodeFunctorTest {
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
        Explode explode = new Explode(new FieldExpression("abc"), "foo", "bar", true, true);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        Assert.assertTrue(functor.field instanceof FieldEvaluator);
        Assert.assertEquals(functor.keyAlias, "foo");
        Assert.assertEquals(functor.valueAlias, "bar");
        Assert.assertTrue(functor.lateralView);
        Assert.assertTrue(functor.outer);
    }

    @Test
    public void testApplyToList() {
        Explode explode = new Explode(new FieldExpression("listA"), "foo", null, false, false);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 3);
        Assert.assertEquals(records.get(0).fieldCount(), 1);
        Assert.assertEquals(records.get(0).typedGet("foo").getValue(), 0);
        Assert.assertEquals(records.get(1).fieldCount(), 1);
        Assert.assertEquals(records.get(1).typedGet("foo").getValue(), 1);
        Assert.assertEquals(records.get(2).fieldCount(), 1);
        Assert.assertEquals(records.get(2).typedGet("foo").getValue(), 2);
    }

    @Test
    public void testApplyToEmptyList() {
        Explode explode = new Explode(new FieldExpression("listB"), "foo", null, false, false);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 0);
    }

    @Test
    public void testApplyToNotList() {
        Explode explode = new Explode(new FieldExpression("mapA"), "foo", null, false, false);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 0);
    }

    @Test
    public void testApplyToMap() {
        Explode explode = new Explode(new FieldExpression("mapA"), "foo", "bar", false, false);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 3);
        Assert.assertEquals(records.get(0).fieldCount(), 2);
        Assert.assertEquals(records.get(0).typedGet("foo").getValue(), "a");
        Assert.assertEquals(records.get(0).typedGet("bar").getValue(), 0);
        Assert.assertEquals(records.get(1).fieldCount(), 2);
        Assert.assertEquals(records.get(1).typedGet("foo").getValue(), "b");
        Assert.assertEquals(records.get(1).typedGet("bar").getValue(), 1);
        Assert.assertEquals(records.get(2).fieldCount(), 2);
        Assert.assertEquals(records.get(2).typedGet("foo").getValue(), "c");
        Assert.assertEquals(records.get(2).typedGet("bar").getValue(), 2);
    }

    @Test
    public void testApplyToEmptyMap() {
        Explode explode = new Explode(new FieldExpression("mapB"), "foo", "bar", false, false);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 0);
    }

    @Test
    public void testApplyToNotMap() {
        Explode explode = new Explode(new FieldExpression("listA"), "foo", "bar", false, false);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 0);
    }

    @Test
    public void testApplyWithBadEvaluator() {
        Explode explode = new Explode(new UnaryExpression(new ValueExpression(5), Operation.SIZE_OF), "foo", "bar", false, false);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.apply(record, provider);

        Assert.assertEquals(records.size(), 0);
    }

    @Test
    public void testGetRecordsWithOuter() {
        Explode explode = new Explode(new FieldExpression("mapA"), "foo", "bar", false, true);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.getRecords(record, provider);

        Assert.assertEquals(records.size(), 3);
        Assert.assertEquals(records.get(0).fieldCount(), 2);
        Assert.assertEquals(records.get(0).typedGet("foo").getValue(), "a");
        Assert.assertEquals(records.get(0).typedGet("bar").getValue(), 0);
        Assert.assertEquals(records.get(1).fieldCount(), 2);
        Assert.assertEquals(records.get(1).typedGet("foo").getValue(), "b");
        Assert.assertEquals(records.get(1).typedGet("bar").getValue(), 1);
        Assert.assertEquals(records.get(2).fieldCount(), 2);
        Assert.assertEquals(records.get(2).typedGet("foo").getValue(), "c");
        Assert.assertEquals(records.get(2).typedGet("bar").getValue(), 2);

        explode = new Explode(new FieldExpression("mapB"), "foo", "bar", false, true);
        functor = new ExplodeFunctor(explode);

        records = functor.getRecords(record, provider);

        Assert.assertEquals(records.size(), 1);
        Assert.assertEquals(records.get(0).fieldCount(), 0);
    }

    @Test
    public void testGetRecordsWithLateralView() {
        Explode explode = new Explode(new FieldExpression("mapA"), "foo", "bar", true, false);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.getRecords(record, provider);

        Assert.assertEquals(records.size(), 3);
        Assert.assertEquals(records.get(0).fieldCount(), 6);
        Assert.assertEquals(records.get(0).typedGet("foo").getValue(), "a");
        Assert.assertEquals(records.get(0).typedGet("bar").getValue(), 0);
        Assert.assertTrue(records.get(0).hasField("listA"));
        Assert.assertTrue(records.get(0).hasField("listB"));
        Assert.assertTrue(records.get(0).hasField("mapA"));
        Assert.assertTrue(records.get(0).hasField("mapB"));
        Assert.assertEquals(records.get(1).fieldCount(), 6);
        Assert.assertEquals(records.get(1).typedGet("foo").getValue(), "b");
        Assert.assertEquals(records.get(1).typedGet("bar").getValue(), 1);
        Assert.assertTrue(records.get(1).hasField("listA"));
        Assert.assertTrue(records.get(1).hasField("listB"));
        Assert.assertTrue(records.get(1).hasField("mapA"));
        Assert.assertTrue(records.get(1).hasField("mapB"));
        Assert.assertEquals(records.get(2).fieldCount(), 6);
        Assert.assertEquals(records.get(2).typedGet("foo").getValue(), "c");
        Assert.assertEquals(records.get(2).typedGet("bar").getValue(), 2);
        Assert.assertTrue(records.get(2).hasField("listA"));
        Assert.assertTrue(records.get(2).hasField("listB"));
        Assert.assertTrue(records.get(2).hasField("mapA"));
        Assert.assertTrue(records.get(2).hasField("mapB"));

        explode = new Explode(new FieldExpression("mapB"), "foo", "bar", true, false);
        functor = new ExplodeFunctor(explode);

        records = functor.getRecords(record, provider);

        Assert.assertEquals(records.size(), 0);
    }

    @Test
    public void testGetRecordsWithLateralViewOuter() {
        Explode explode = new Explode(new FieldExpression("mapB"), "foo", "bar", true, true);
        ExplodeFunctor functor = new ExplodeFunctor(explode);

        List<BulletRecord> records = functor.getRecords(record, provider);

        Assert.assertEquals(records.size(), 1);
        Assert.assertEquals(records.get(0).fieldCount(), 4);
        Assert.assertTrue(records.get(0).hasField("listA"));
        Assert.assertTrue(records.get(0).hasField("listB"));
        Assert.assertTrue(records.get(0).hasField("mapA"));
        Assert.assertTrue(records.get(0).hasField("mapB"));
    }
}
