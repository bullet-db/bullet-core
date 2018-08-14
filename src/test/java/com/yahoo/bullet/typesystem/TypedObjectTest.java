/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.typesystem;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class TypedObjectTest {
    @Test
    public void testTypedObjectWithUnsupportedType() {
        TypedObject object = new TypedObject(Type.UNKNOWN, Collections.emptyList());
        Assert.assertEquals(object.getType(), Type.UNKNOWN);
        Assert.assertEquals(object.getValue(), Collections.emptyList());
    }

    @Test
    public void testToString() {
        TypedObject object = new TypedObject(null);
        Assert.assertEquals(object.getType(), Type.NULL);
        Assert.assertEquals(object.toString(), Type.NULL_EXPRESSION);
        object = new TypedObject("foo");
        Assert.assertEquals(object.getType(), Type.STRING);
        Assert.assertEquals(object.toString(), "foo");
    }

    @Test
    public void testTypeCasting() {
        TypedObject object = new TypedObject(1);
        TypedObject castedToObjectType = object.typeCast("1234");
        Assert.assertEquals(castedToObjectType.getType(), Type.INTEGER);
        Assert.assertEquals(castedToObjectType.getValue(), 1234);

        object = new TypedObject(1L);
        castedToObjectType = object.typeCast("1234");
        Assert.assertEquals(castedToObjectType.getType(), Type.LONG);
        Assert.assertEquals(castedToObjectType.getValue(), 1234L);

        object = new TypedObject(1.123f);
        castedToObjectType = object.typeCast("1234");
        Assert.assertEquals(castedToObjectType.getType(), Type.FLOAT);
        Assert.assertEquals(castedToObjectType.getValue(), 1234f);

        object = new TypedObject(1.123);
        castedToObjectType = object.typeCast("1234");
        Assert.assertEquals(castedToObjectType.getType(), Type.DOUBLE);
        Assert.assertEquals(castedToObjectType.getValue(), 1234.0);

        object = new TypedObject(true);
        castedToObjectType = object.typeCast("false");
        Assert.assertEquals(castedToObjectType.getType(), Type.BOOLEAN);
        Assert.assertEquals(castedToObjectType.getValue(), false);

        object = new TypedObject("foo");
        castedToObjectType = object.typeCast("false");
        Assert.assertEquals(castedToObjectType.getType(), Type.STRING);
        Assert.assertEquals(castedToObjectType.getValue(), "false");
    }

    @Test
    public void testFailTypeCasting() {
        TypedObject object;
        TypedObject casted;

        object = new TypedObject(1L);
        casted = object.typeCast("1234.0");
        Assert.assertEquals(casted.getType(), Type.UNKNOWN);
        Assert.assertNull(casted.getValue());

        object = new TypedObject(Type.MAP, Collections.emptyMap());
        casted = object.typeCast("{}");
        Assert.assertEquals(casted.getType(), Type.UNKNOWN);
        Assert.assertNull(casted.getValue());

        object = new TypedObject(Type.LIST, Collections.emptyList());
        casted = object.typeCast("[]");
        Assert.assertEquals(casted.getType(), Type.UNKNOWN);
        Assert.assertNull(casted.getValue());
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testUnUnifiedTypeComparison() {
        TypedObject objectA = new TypedObject(1L);
        TypedObject objectB = new TypedObject("1");
        objectA.compareTo(objectB);
    }

    @Test
    public void testBooleanComparison() {
        TypedObject objectA = new TypedObject(true);
        TypedObject objectB = new TypedObject(false);
        Assert.assertTrue(objectA.compareTo(objectB) > 0);
        Assert.assertTrue(objectB.compareTo(objectA) < 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testStringComparison() {
        TypedObject objectA = new TypedObject("foo");
        TypedObject objectB = new TypedObject("bar");
        Assert.assertTrue(objectA.compareTo(objectB) > 0);
        Assert.assertTrue(objectB.compareTo(objectA) < 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testIntegerComparison() {
        TypedObject objectA = new TypedObject(42);
        TypedObject objectB = new TypedObject(43);
        Assert.assertTrue(objectA.compareTo(objectB) < 0);
        Assert.assertTrue(objectB.compareTo(objectA) > 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testLongComparison() {
        TypedObject objectA = new TypedObject(42L);
        TypedObject objectB = new TypedObject(43L);
        Assert.assertTrue(objectA.compareTo(objectB) < 0);
        Assert.assertTrue(objectB.compareTo(objectA) > 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testFloatComparison() {
        TypedObject objectA = new TypedObject(42.0f);
        TypedObject objectB = new TypedObject(42.1f);
        Assert.assertTrue(objectA.compareTo(objectB) < 0);
        Assert.assertTrue(objectB.compareTo(objectA) > 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testDoubleComparison() {
        TypedObject objectA = new TypedObject(42.0);
        TypedObject objectB = new TypedObject(42.1);
        Assert.assertTrue(objectA.compareTo(objectB) < 0);
        Assert.assertTrue(objectB.compareTo(objectA) > 0);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test
    public void testUnknownComparison() {
        TypedObject objectA = new TypedObject(Type.UNKNOWN, null);
        TypedObject objectB = new TypedObject(42.1);
        Assert.assertEquals(objectA.compareTo(objectB), Integer.MIN_VALUE);
        Assert.assertEquals(objectA.compareTo(objectA), Integer.MIN_VALUE);
    }

    @Test
    public void testNullComparisonToOthers() {
        TypedObject objectA = new TypedObject(null);
        TypedObject objectB = new TypedObject(42.1);
        TypedObject objectC = new TypedObject("foo");
        Assert.assertEquals(objectA.compareTo(objectB), Integer.MIN_VALUE);
        Assert.assertEquals(objectA.compareTo(objectC), Integer.MIN_VALUE);
        Assert.assertTrue(objectA.compareTo(objectA) == 0);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testOtherComparisonToNull() {
        TypedObject nullObject = new TypedObject(null);
        TypedObject object = new TypedObject(42.1);
        object.compareTo(nullObject);
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testOtherComparisontoUnknown() {
        TypedObject objectA = new TypedObject(Collections.emptyList());
        TypedObject objectB = new TypedObject(42.1);
        objectB.compareTo(objectA);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type cannot .*")
    public void testUnsupportedTypeComparison() {
        TypedObject object = new TypedObject(Type.MAP, Collections.emptyMap());
        object.compareTo(new TypedObject(42L));
    }

    @Test
    public void testCastingToNumber() {
        Object value;

        TypedObject objectA = TypedObject.makeNumber("42.1");
        value = objectA.getValue();
        Assert.assertEquals(objectA.getType(), Type.DOUBLE);
        Assert.assertNotNull(value);
        Assert.assertTrue(Number.class.isInstance(value));
        Assert.assertEquals(value, 42.1);

        TypedObject objectB = TypedObject.makeNumber("42");
        value = objectB.getValue();
        Assert.assertEquals(objectB.getType(), Type.DOUBLE);
        Assert.assertNotNull(value);
        Assert.assertTrue(Number.class.isInstance(value));
        Assert.assertEquals(value, 42.0);

        TypedObject objectC = TypedObject.makeNumber("{}");
        Assert.assertEquals(objectC.getType(), Type.UNKNOWN);
        Assert.assertNull(objectC.getValue());

        TypedObject objectD = TypedObject.makeNumber(Collections.emptyList());
        Assert.assertEquals(objectD.getType(), Type.UNKNOWN);
        Assert.assertNull(objectD.getValue());

        TypedObject objectE = TypedObject.makeNumber(Collections.emptyMap());
        Assert.assertEquals(objectE.getType(), Type.UNKNOWN);
        Assert.assertNull(objectE.getValue());

        TypedObject objectF = TypedObject.makeNumber(null);
        Assert.assertEquals(objectF.getType(), Type.UNKNOWN);
        Assert.assertNull(objectF.getValue());
    }

    @Test
    public void testExtractPrimitiveType() {
        TypedObject objectA = new TypedObject("");
        TypedObject objectB = new TypedObject(Arrays.asList("1", "2"));
        TypedObject objectC = new TypedObject(Collections.emptyList());
        TypedObject objectD = new TypedObject(Collections.emptyMap());
        TypedObject objectE = new TypedObject(Collections.singletonMap("1", "2"));
        TypedObject objectF = new TypedObject(Collections.singletonMap("11", Collections.singletonMap("1", "2")));
        Assert.assertEquals(objectA.getPrimitiveType(), Type.UNKNOWN);
        Assert.assertEquals(objectB.getPrimitiveType(), Type.STRING);
        Assert.assertEquals(objectC.getPrimitiveType(), Type.UNKNOWN);
        Assert.assertEquals(objectD.getPrimitiveType(), Type.UNKNOWN);
        Assert.assertEquals(objectE.getPrimitiveType(), Type.STRING);
        Assert.assertEquals(objectF.getPrimitiveType(), Type.STRING);
    }

    @Test
    public void testSize() {
        TypedObject objectA = new TypedObject(Arrays.asList("1", "2"));
        TypedObject objectB = new TypedObject(Collections.emptyList());
        TypedObject objectC = new TypedObject("");
        TypedObject objectD = new TypedObject("11");
        Assert.assertEquals(objectA.size().intValue(), 2);
        Assert.assertEquals(objectB.size().intValue(), 0);
        Assert.assertEquals(objectC.size().intValue(), 0);
        Assert.assertEquals(objectD.size().intValue(), 2);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*This type of field does not support size of.*")
    public void testUnsupportedTypeSize() {
        TypedObject object = new TypedObject(1);
        object.size();
    }

    @Test
    public void testContainsKey() {
        TypedObject objectB = new TypedObject(Collections.emptyList());
        TypedObject objectC = new TypedObject(Arrays.asList(Collections.emptyMap()));
        TypedObject objectD = new TypedObject(Arrays.asList(Collections.singletonMap("1", "2")));
        TypedObject objectE = new TypedObject(Collections.emptyMap());
        TypedObject objectF = new TypedObject(Collections.singletonMap("1", "2"));
        TypedObject objectG = new TypedObject(Collections.singletonMap("11", Collections.singletonMap("1", "2")));
        Assert.assertFalse(objectB.containsKey("1"));
        Assert.assertFalse(objectC.containsKey("1"));
        Assert.assertTrue(objectD.containsKey("1"));
        Assert.assertFalse(objectE.containsKey("1"));
        Assert.assertTrue(objectF.containsKey("1"));
        Assert.assertFalse(objectF.containsKey("2"));
        Assert.assertTrue(objectG.containsKey("1"));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*This type of field does not support contains key.*")
    public void testUnsupportedTypeContainsKey() {
        TypedObject object = new TypedObject(1);
        object.containsKey("1");
    }

    @Test
    public void testContainsValue() {
        TypedObject objectA = new TypedObject(Arrays.asList("1", "2"));
        TypedObject objectB = new TypedObject(Collections.emptyList());
        TypedObject objectC = new TypedObject(Arrays.asList(Collections.emptyMap()));
        TypedObject objectD = new TypedObject(Arrays.asList(Collections.singletonMap("1", "2")));
        TypedObject objectE = new TypedObject(Collections.emptyMap());
        TypedObject objectF = new TypedObject(Collections.singletonMap("1", "2"));
        TypedObject objectG = new TypedObject(Collections.singletonMap("11", Collections.singletonMap("1", "2")));
        Assert.assertFalse(objectA.containsValue(new TypedObject("3")));
        Assert.assertFalse(objectB.containsValue(new TypedObject("1")));
        Assert.assertFalse(objectC.containsValue(new TypedObject("1")));
        Assert.assertFalse(objectD.containsValue(new TypedObject("1")));
        Assert.assertTrue(objectD.containsValue(new TypedObject("2")));
        Assert.assertFalse(objectE.containsValue(new TypedObject("1")));
        Assert.assertFalse(objectF.containsValue(new TypedObject("1")));
        Assert.assertTrue(objectF.containsValue(new TypedObject("2")));
        Assert.assertFalse(objectG.containsValue(new TypedObject("1")));
        Assert.assertTrue(objectG.containsValue(new TypedObject("2")));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*This type of field does not support contains value.*")
    public void testUnsupportedTypeContainsValue() {
        TypedObject object = new TypedObject(1);
        object.containsValue(new TypedObject("1"));
    }

    @Test
    public void testTypeCastFromObject() {
        TypedObject object = new TypedObject(42L);
        Long longNum = 50L;
        TypedObject casted = object.typeCastFromObject(longNum);
        Assert.assertEquals(casted.getType(), Type.LONG);
        Assert.assertEquals(casted.getValue(), 50L);

        object = new TypedObject("str");
        String str = "test";
        casted = object.typeCastFromObject(str);
        Assert.assertEquals(casted.getType(), Type.STRING);
        Assert.assertEquals(casted.getValue(), "test");

        object = new TypedObject(42L);
        Integer integer = 50;
        casted = object.typeCastFromObject(integer);
        Assert.assertEquals(casted.getType(), Type.UNKNOWN);

        casted = object.typeCastFromObject(null);
        Assert.assertEquals(casted.getType(), Type.UNKNOWN);

        object = new TypedObject(42);
        casted = object.typeCastFromObject(longNum);
        Assert.assertEquals(casted.getType(), Type.UNKNOWN);
    }
}
