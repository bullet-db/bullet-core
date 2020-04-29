/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static com.yahoo.bullet.querying.evaluators.EvaluatorUtils.valueEvaluator;

public class NAryOperationsTest {
    @Test
    public void testConstructor() {
        // coverage only
        new NAryOperations();
    }

    @Test
    public void testAllMatch() {
        Assert.assertEquals(NAryOperations.ALL_MATCH.apply(Arrays.asList(valueEvaluator(true), valueEvaluator(true)), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(NAryOperations.ALL_MATCH.apply(Arrays.asList(valueEvaluator(true), valueEvaluator(false)), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(NAryOperations.ALL_MATCH.apply(Arrays.asList(valueEvaluator(false), valueEvaluator(false)), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(NAryOperations.ALL_MATCH.apply(new ArrayList<>(), null), new TypedObject(Type.BOOLEAN, true));
    }

    @Test
    public void testAnyMatch() {
        Assert.assertEquals(NAryOperations.ANY_MATCH.apply(Arrays.asList(valueEvaluator(true), valueEvaluator(true)), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(NAryOperations.ANY_MATCH.apply(Arrays.asList(valueEvaluator(true), valueEvaluator(false)), null), new TypedObject(Type.BOOLEAN, true));
        Assert.assertEquals(NAryOperations.ANY_MATCH.apply(Arrays.asList(valueEvaluator(false), valueEvaluator(false)), null), new TypedObject(Type.BOOLEAN, false));
        Assert.assertEquals(NAryOperations.ANY_MATCH.apply(new ArrayList<>(), null), new TypedObject(Type.BOOLEAN, false));
    }

    @Test
    public void testIf() {
        Assert.assertEquals(NAryOperations.IF.apply(Arrays.asList(valueEvaluator(true), valueEvaluator(1), valueEvaluator(2)), null), new TypedObject(Type.INTEGER, 1));
        Assert.assertEquals(NAryOperations.IF.apply(Arrays.asList(valueEvaluator(false), valueEvaluator(1), valueEvaluator(2)), null), new TypedObject(Type.INTEGER, 2));
    }
}
