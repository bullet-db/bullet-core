/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class FieldEvaluatorTest {
    private HashMap<String, Serializable> map;
    private BulletRecord record;

    @BeforeClass
    public void setup() {
        map = new HashMap<>();
        map.put("def", 5);
        record = RecordBox.get().addListOfMaps("abc", map)
                                .addMapOfMaps("aaa", Pair.of("abc", map))
                                .add("a", 0)
                                .add("b", "abc")
                                .add("c", "def")
                                .getRecord();
    }

    @Test
    public void testEvaluate() {
        FieldEvaluator evaluator = new FieldEvaluator(new FieldExpression("abc"));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER_MAP_LIST, new ArrayList<>(Collections.singletonList(map))));

        evaluator = new FieldEvaluator(new FieldExpression("abc", 0));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER_MAP, map));

        evaluator = new FieldEvaluator(new FieldExpression("abc", 0, "def"));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER, 5));

        HashMap<String, Serializable> hashMap = new HashMap<>();
        hashMap.put("abc", map);

        evaluator = new FieldEvaluator(new FieldExpression("aaa"));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER_MAP_MAP, hashMap));

        evaluator = new FieldEvaluator(new FieldExpression("aaa", "abc"));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER_MAP, map));

        evaluator = new FieldEvaluator(new FieldExpression("aaa", "abc", "def"));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER, 5));
    }

    @Test
    public void testEvaluateWithExpressionsMixedIn() {
        FieldEvaluator evaluator = new FieldEvaluator(new FieldExpression("abc", new FieldExpression("a")));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER_MAP, map));

        evaluator = new FieldEvaluator(new FieldExpression("abc", new FieldExpression("a"), new FieldExpression("c")));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER, 5));

        evaluator = new FieldEvaluator(new FieldExpression("abc", 0, new FieldExpression("c")));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER, 5));

        evaluator = new FieldEvaluator(new FieldExpression("abc", new FieldExpression("a"), "def"));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER, 5));

        evaluator = new FieldEvaluator(new FieldExpression("aaa", new FieldExpression("b")));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER_MAP, map));

        evaluator = new FieldEvaluator(new FieldExpression("aaa", new FieldExpression("b"), new FieldExpression("c")));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER, 5));

        evaluator = new FieldEvaluator(new FieldExpression("aaa", "abc", new FieldExpression("c")));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER, 5));

        evaluator = new FieldEvaluator(new FieldExpression("aaa", new FieldExpression("b"), "def"));
        Assert.assertEquals(evaluator.evaluate(record), new TypedObject(Type.INTEGER, 5));
    }

    @Test
    public void testEvaluateWithExpressionsAndNulls() {
        FieldEvaluator evaluator = new FieldEvaluator(new FieldExpression("abc", new FieldExpression("dne")));
        Assert.assertEquals(evaluator.evaluate(record), TypedObject.NULL);

        evaluator = new FieldEvaluator(new FieldExpression("abc", new FieldExpression("dne"), new FieldExpression("c")));
        Assert.assertEquals(evaluator.evaluate(record), TypedObject.NULL);

        evaluator = new FieldEvaluator(new FieldExpression("abc", new FieldExpression("a"), new FieldExpression("dne")));
        Assert.assertEquals(evaluator.evaluate(record), TypedObject.NULL);

        evaluator = new FieldEvaluator(new FieldExpression("abc", 0, new FieldExpression("dne")));
        Assert.assertEquals(evaluator.evaluate(record), TypedObject.NULL);

        evaluator = new FieldEvaluator(new FieldExpression("abc", new FieldExpression("dne"), "def"));
        Assert.assertEquals(evaluator.evaluate(record), TypedObject.NULL);

        evaluator = new FieldEvaluator(new FieldExpression("aaa", "abc", new FieldExpression("dne")));
        Assert.assertEquals(evaluator.evaluate(record), TypedObject.NULL);
    }
}
