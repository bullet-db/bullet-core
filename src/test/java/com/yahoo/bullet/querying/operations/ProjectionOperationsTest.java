/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.operations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Projection;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.yahoo.bullet.parsing.ProjectionUtils.makeProjection;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class ProjectionOperationsTest {
    private static BulletRecordProvider provider = new BulletConfig().getBulletRecordProvider();

    @Test
    public void testDefaults() {
        Projection projection = new Projection();
        Assert.assertNull(projection.getFields());

        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        BulletRecord actual = ProjectionOperations.project(record, projection, null, provider);
        BulletRecord expected = RecordBox.get().add("foo", "bar").getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testProjection() {
        Projection projection = makeProjection("map_field.foo", "bar");
        RecordBox box = RecordBox.get().addMap("map_field", Pair.of("foo", "baz"));
        BulletRecord actual = ProjectionOperations.project(box.getRecord(), projection, null, provider);
        BulletRecord expected = RecordBox.get().add("bar", "baz").getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testUnsupportedProjection() {
        Projection projection = makeProjection(ImmutablePair.of("list_field.1.foo", "bar"),
                                               ImmutablePair.of("field", "foo"));
        BulletRecord record = RecordBox.get().addList("list_field", emptyMap(), singletonMap("foo", "bar"))
                                             .add("field", "123")
                                             .getRecord();
        BulletRecord actual = ProjectionOperations.project(record, projection, null, provider);
        BulletRecord expected = RecordBox.get().add("foo", "123").getRecord();
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testMapList() {
        Projection projection = makeProjection("list_field", "foo");

        BulletRecord record = RecordBox.get().addList("list_field", emptyMap(), singletonMap("foo", "baz")).getRecord();

        BulletRecord expected = RecordBox.get().addList("foo", emptyMap(), singletonMap("foo", "baz")).getRecord();

        BulletRecord actual = ProjectionOperations.project(record, projection, null, provider);
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testRepeatedProjections() {
        Projection first = makeProjection("field", "id");
        Projection second = makeProjection("map_field.foo", "bar");

        RecordBox box = RecordBox.get().add("field", "test").addMap("map_field", Pair.of("foo", "baz"));

        BulletRecord record = box.getRecord();
        BulletRecord firstProjection = ProjectionOperations.project(record, first, null, provider);
        BulletRecord secondProjection = ProjectionOperations.project(record, second, null, provider);

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
        Projection projection = makeProjection(ImmutablePair.of(null, "test"), ImmutablePair.of("map_field.foo", "foo"));

        BulletRecord record = RecordBox.get().add("field", "test").addMap("map_field", Pair.of("foo", "baz")).getRecord();

        BulletRecord actual = ProjectionOperations.project(record, projection, null, provider);
        BulletRecord expected = RecordBox.get().add("foo", "baz").getRecord();
        Assert.assertEquals(actual, expected);
    }
}
