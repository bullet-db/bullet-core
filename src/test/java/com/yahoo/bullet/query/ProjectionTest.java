/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

@SuppressWarnings("unchecked")
public class ProjectionTest {
    /*
    @Test
    public void testDefaults() {
        Projection projection = new Projection();
        Assert.assertNull(projection.getFields());
    }

    @Test
    public void testProjection() {
        Projection projection = new Projection();
        projection.setFields(singletonMap("map_field.foo", "bar"));
        Assert.assertNotNull(projection.getFields());
    }

    @Test
    public void testInitialize() {
        Projection projection = new Projection();
        Optional<List<BulletError>> errors = projection.initialize();
        Assert.assertFalse(errors.isPresent());
    }

    @Test
    public void testToString() {
        Projection projection = new Projection();

        Assert.assertEquals(projection.toString(), "{fields: null}");

        projection.setFields(emptyMap());
        Assert.assertEquals(projection.toString(), "{fields: {}}");

        Map<String, String> fields = new HashMap<>();
        fields.put(null, "test");
        fields.put("map_field.foo", "foo");
        projection.setFields(fields);

        Assert.assertEquals(projection.toString(), "{fields: {null=test, map_field.foo=foo}}");
    }
    */
}
