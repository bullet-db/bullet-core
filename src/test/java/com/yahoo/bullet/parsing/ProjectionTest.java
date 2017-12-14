/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

@SuppressWarnings("unchecked")
public class ProjectionTest {
    @Test
    public void testDefaults() {
        Projection projection = new Projection();
        Assert.assertNull(projection.getFields());

        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        BulletRecord actual = projection.project(record);
        BulletRecord expected = RecordBox.get().add("foo", "bar").getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testProjection() {
        Projection projection = new Projection();
        projection.setFields(singletonMap("map_field.foo", "bar"));
        RecordBox box = RecordBox.get().addMap("map_field", Pair.of("foo", "baz"));
        BulletRecord actual = projection.project(box.getRecord());
        BulletRecord expected = RecordBox.get().add("bar", "baz").getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testUnsupportedProjection() {
        Projection projection = new Projection();
        Map<String, String> fields = new HashMap<>();
        fields.put("list_field.1.foo", "bar");
        fields.put("field", "foo");
        projection.setFields(fields);
        BulletRecord record = RecordBox.get().addList("list_field", emptyMap(), singletonMap("foo", "bar"))
                                             .add("field", "123")
                                             .getRecord();
        BulletRecord actual = projection.project(record);
        BulletRecord expected = RecordBox.get().add("foo", "123").getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testMapList() {
        Projection projection = new Projection();
        projection.setFields(singletonMap("list_field", "foo"));

        BulletRecord record = RecordBox.get().addList("list_field", emptyMap(), singletonMap("foo", "baz"))
                                             .getRecord();

        BulletRecord expected = RecordBox.get().addList("foo", emptyMap(), singletonMap("foo", "baz"))
                                               .getRecord();

        BulletRecord actual = projection.project(record);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testRepeatedProjections() {
        Projection first = new Projection();
        first.setFields(singletonMap("field", "id"));

        Projection second = new Projection();
        second.setFields(singletonMap("map_field.foo", "bar"));

        RecordBox box = RecordBox.get().add("field", "test").addMap("map_field", Pair.of("foo", "baz"));

        BulletRecord record = box.getRecord();
        BulletRecord firstProjection = first.project(record);
        BulletRecord secondProjection = second.project(record);

        box = RecordBox.get().add("field", "test").addMap("map_field", Pair.of("foo", "baz"));
        BulletRecord expectedOriginal = box.getRecord();
        Assert.assertEquals(record, expectedOriginal);

        box = RecordBox.get().add("id", "test");
        BulletRecord expectedFirstProjection = box.getRecord();
        Assert.assertEquals(firstProjection, expectedFirstProjection);

        box = RecordBox.get().add("bar", "baz");
        BulletRecord expectedSecondProjection = box.getRecord();
        Assert.assertEquals(secondProjection, expectedSecondProjection);
    }

    @Test
    public void testNullFieldName() {
        Projection projection = new Projection();
        Map<String, String> fields = new HashMap<>();
        fields.put(null, "test");
        fields.put("map_field.foo", "foo");
        projection.setFields(fields);

        BulletRecord record = RecordBox.get().add("field", "test").addMap("map_field", Pair.of("foo", "baz")).getRecord();

        BulletRecord actual = projection.project(record);
        BulletRecord expected = RecordBox.get().add("foo", "baz").getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testValidate() {
        Projection projection = new Projection();
        Optional<List<Error>> errors = projection.initialize();
        // currently Projection.normalize() does nothing
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
}
