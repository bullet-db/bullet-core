/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static com.yahoo.bullet.querying.evaluators.EvaluatorUtils.valueEvaluator;

public class NAryOperationsTest {
    @Test
    public void testConstructor() {
        // coverage only
        new NAryOperations();
    }

    @Test
    public void testAllMatch() {
        Assert.assertEquals(NAryOperations.allMatch(Arrays.asList(valueEvaluator(true), valueEvaluator(true)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.allMatch(Arrays.asList(valueEvaluator(true), valueEvaluator(false)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.allMatch(Arrays.asList(valueEvaluator(false), valueEvaluator(false)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.allMatch(Arrays.asList(valueEvaluator(null), valueEvaluator(false)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.allMatch(Arrays.asList(valueEvaluator(null), valueEvaluator(true)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.allMatch(new ArrayList<>(), null), TypedObject.TRUE);
    }

    @Test
    public void testAnyMatch() {
        Assert.assertEquals(NAryOperations.anyMatch(Arrays.asList(valueEvaluator(true), valueEvaluator(true)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.anyMatch(Arrays.asList(valueEvaluator(true), valueEvaluator(false)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.anyMatch(Arrays.asList(valueEvaluator(false), valueEvaluator(false)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.anyMatch(Arrays.asList(valueEvaluator(null), valueEvaluator(true)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.anyMatch(Arrays.asList(valueEvaluator(null), valueEvaluator(false)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.anyMatch(new ArrayList<>(), null), TypedObject.FALSE);
    }

    @Test
    public void testIf() {
        Assert.assertEquals(NAryOperations.ternary(Arrays.asList(valueEvaluator(true), valueEvaluator(1), valueEvaluator(2)), null), new TypedObject(Type.INTEGER, 1));
        Assert.assertEquals(NAryOperations.ternary(Arrays.asList(valueEvaluator(false), valueEvaluator(1), valueEvaluator(2)), null), new TypedObject(Type.INTEGER, 2));
        Assert.assertEquals(NAryOperations.ternary(Arrays.asList(valueEvaluator(null), valueEvaluator(1), valueEvaluator(2)), null), new TypedObject(Type.INTEGER, 2));
    }

    @Test
    public void testBetween() {
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(5), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(7), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(10), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(4), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(11), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(7), valueEvaluator(10), valueEvaluator(5)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(null), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(null), valueEvaluator(10), valueEvaluator(5)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(10), valueEvaluator(null), valueEvaluator(10)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(11), valueEvaluator(null), valueEvaluator(10)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(5), valueEvaluator(5), valueEvaluator(null)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.between(Arrays.asList(valueEvaluator(4), valueEvaluator(5), valueEvaluator(null)), null), TypedObject.FALSE);
    }

    @Test
    public void testNotBetween() {
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(5), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(7), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(10), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.FALSE);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(4), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(11), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(7), valueEvaluator(10), valueEvaluator(5)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(null), valueEvaluator(5), valueEvaluator(10)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(null), valueEvaluator(10), valueEvaluator(5)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(10), valueEvaluator(null), valueEvaluator(10)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(11), valueEvaluator(null), valueEvaluator(10)), null), TypedObject.TRUE);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(5), valueEvaluator(5), valueEvaluator(null)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.notBetween(Arrays.asList(valueEvaluator(4), valueEvaluator(5), valueEvaluator(null)), null), TypedObject.TRUE);
    }

    @Test
    public void testSubstring() {
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(1)), null), TypedObject.valueOf("hello world"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(7)), null), TypedObject.valueOf("world"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(11)), null), TypedObject.valueOf("d"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(12)), null), TypedObject.valueOf(""));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(-1)), null), TypedObject.valueOf("d"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(-5)), null), TypedObject.valueOf("world"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(-11)), null), TypedObject.valueOf("hello world"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(-12)), null), TypedObject.valueOf(""));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(1), valueEvaluator(5)), null), TypedObject.valueOf("hello"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(1), valueEvaluator(11)), null), TypedObject.valueOf("hello world"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(1), valueEvaluator(12)), null), TypedObject.valueOf("hello world"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(6), valueEvaluator(1)), null), TypedObject.valueOf(" "));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(-5), valueEvaluator(5)), null), TypedObject.valueOf("world"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(-5), valueEvaluator(11)), null), TypedObject.valueOf("world"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(-11), valueEvaluator(12)), null), TypedObject.valueOf("hello world"));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(-6), valueEvaluator(1)), null), TypedObject.valueOf(" "));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(1), valueEvaluator(0)), null), TypedObject.valueOf(""));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(1), valueEvaluator(-1)), null), TypedObject.valueOf(""));
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator(null), valueEvaluator(1)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(null)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.substring(Arrays.asList(valueEvaluator("hello world"), valueEvaluator(1), valueEvaluator(null)), null), TypedObject.NULL);
    }

    @Test
    public void testUnixTimestamp() {
        long timeBefore = System.currentTimeMillis() / 1000;
        TypedObject unixTimestampA = NAryOperations.unixTimestamp(Collections.emptyList(), null);
        long timeA = ((Number) unixTimestampA.getValue()).longValue();
        long timeAfter = System.currentTimeMillis() / 1000;
        Assert.assertTrue(timeBefore <= timeA && timeA <= timeAfter);

        TypedObject unixTimestampB = NAryOperations.unixTimestamp(Collections.singletonList(valueEvaluator("2021-01-01 00:00:00")), null);
        long timeB = ((Number) unixTimestampB.getValue()).longValue();
        // Account for timezone range of system running the test. GMT-12 <= timeB <= GMT+12
        Assert.assertTrue(1609459200 - 12 * 60 * 60 <= timeB && timeB <= 1609459200 + 12 * 60 * 60);

        TypedObject unixTimestampC = NAryOperations.unixTimestamp(Arrays.asList(valueEvaluator("2021010100"), valueEvaluator("yyyyMMddHH")), null);
        long timeC = ((Number) unixTimestampC.getValue()).longValue();
        Assert.assertEquals(timeC, timeB);

        TypedObject unixTimestampD = NAryOperations.unixTimestamp(Arrays.asList(valueEvaluator(2021010100), valueEvaluator("yyyyMMddHH")), null);
        long timeD = ((Number) unixTimestampD.getValue()).longValue();
        Assert.assertEquals(timeD, timeB);

        Assert.assertEquals(NAryOperations.unixTimestamp(Collections.singletonList(valueEvaluator(null)), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.unixTimestamp(Arrays.asList(valueEvaluator(null), valueEvaluator("yyyyMMddHH")), null), TypedObject.NULL);
        Assert.assertEquals(NAryOperations.unixTimestamp(Arrays.asList(valueEvaluator("2021030519"), valueEvaluator(null)), null), TypedObject.NULL);
    }
}
