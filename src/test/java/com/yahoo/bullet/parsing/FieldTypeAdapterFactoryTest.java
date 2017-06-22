/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class FieldTypeAdapterFactoryTest {
    private static class Base {
        public Integer foo;
        public String bar;
    }

    private static class SubTypeA extends Base {
        public String baz;
    }

    private static class SubTypeB extends Base {
        public List<String> qux;
    }

    private static class SubTypeC extends Base {
        public boolean norf;
    }

    private Gson getGSON(FieldTypeAdapterFactory<Base> factory) {
        return new GsonBuilder().registerTypeAdapterFactory(factory).create();
    }

    private FieldTypeAdapterFactory<Base> getFactory(Function<JsonElement, String> fn, List<String> a, List<String> b) {
        return FieldTypeAdapterFactory.of(Base.class, fn)
                                      .registerSubType(SubTypeA.class, a)
                                      .registerSubType(SubTypeB.class, b) ;
    }

    private String makeJSON(Integer foo, String bar, String baz) {
        return "{" +
                "'foo': "  + foo + "," +
                "'bar': '" + bar + "'," +
                "'baz': '" + baz + "'" +
                "}";
    }

    private String makeJSON(Integer foo, String bar, List<String> qux) {
        return "{" +
               "'foo':"  + foo + "," +
               "'bar':'" + bar + "'," +
               "'qux':['" + qux.stream().reduce((a, b) -> a + "','" + b).orElse("") + "']" +
               "}";
    }

    @Test
    public void testDeserialization() {
        Gson gson = getGSON(getFactory(t -> t.getAsJsonObject().get("bar").getAsString(),
                                       asList("typeA", "A"),
                                       asList("typeB", "B")));

        Base deserializedB = gson.fromJson(makeJSON(1, "B", asList("a", "b")), Base.class);
        SubTypeB castedB = (SubTypeB) deserializedB;
        Assert.assertNotNull(castedB);
        Assert.assertEquals(castedB.foo, Integer.valueOf(1));
        Assert.assertEquals(castedB.bar, "B");
        Assert.assertEquals(castedB.qux, asList("a", "b"));

        Base deserializedA = gson.fromJson(makeJSON(2, "typeA", "test"), Base.class);
        SubTypeA castedA = (SubTypeA) deserializedA;
        Assert.assertNotNull(castedA);
        Assert.assertEquals(castedA.foo, Integer.valueOf(2));
        Assert.assertEquals(castedA.bar, "typeA");
        Assert.assertEquals(castedA.baz, "test");
    }

    @Test
    public void testDeserializationFail() {
        Gson gson = getGSON(getFactory(t -> t.getAsJsonObject().get("bar").getAsString(),
                                       asList("typeA", "A"),
                                       asList("typeB", "B")));
        Base deserialized = gson.fromJson(makeJSON(1, "garbage", "a"), Base.class);
        Assert.assertNull(deserialized);
    }

    @Test
    public void testSerialization() {
        Gson gson = getGSON(getFactory(t -> "foo", emptyList(), emptyList()));
        SubTypeA typeA = new SubTypeA();
        typeA.foo = 1;
        typeA.bar = "t1";
        typeA.baz = "t2";

        JsonElement actual = gson.toJsonTree(typeA, new TypeToken<Base>() { }.getType());
        JsonElement expected = new JsonParser().parse(makeJSON(1, "t1", "t2"));
        Assert.assertEquals(actual, expected);

        SubTypeB typeB = new SubTypeB();
        typeB.foo = 2;
        typeB.bar = "t2";
        typeB.qux = asList("a", "b");

        actual = gson.toJsonTree(typeB, new TypeToken<Base>() { }.getType());
        expected = new JsonParser().parse(makeJSON(2, "t2", asList("a", "b")));
        Assert.assertEquals(actual, expected);
    }

    @Test(expectedExceptions = JsonParseException.class)
    public void testSerializationFail() {
        Gson gson = getGSON(getFactory(t -> "foo", emptyList(), emptyList()));
        // Not registered
        SubTypeC typeC = new SubTypeC();
        typeC.foo = 1;
        typeC.bar = "t1";
        typeC.norf = true;

        gson.toJsonTree(typeC, new TypeToken<Base>() { }.getType());
    }
}
