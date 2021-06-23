/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.result;

import lombok.AllArgsConstructor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;

public class JSONFormatterTest {
    @AllArgsConstructor
    private static class Foo implements JSONFormatter {
        private int bar;
        private Map<String, Double> baz;

        @Override
        public String asJSON() {
            return JSONFormatter.asJSON(this);
        }
    }

    @Test
    public void testToJSON() {
        String json = new Foo(42, Collections.singletonMap("foo", 42.0)).asJSON();
        assertJSONEquals(json, "{'bar': 42, 'baz': {'foo': 42.0 }}");
    }

    @Test
    public void testFromJSON() {
        Foo foo = JSONFormatter.fromJSON("{'bar': 42, 'baz': {'foo': 42.0 }}", Foo.class);
        Assert.assertEquals(foo.bar, 42);
        Assert.assertEquals(foo.baz, Collections.singletonMap("foo", 42.0));
    }
}
