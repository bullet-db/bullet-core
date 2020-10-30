/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.avro.TypedAvroBulletRecordProvider;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class ProjectionTest {
    @Test
    public void testProjectNewRecord() {
        BulletRecord record = RecordBox.get().add("a", 2)
                                             .add("b", 4)
                                             .add("c", 6).getRecord();

        List<Field> fields = Arrays.asList(new Field("d", new BinaryExpression(new FieldExpression("a"),
                                                                               new FieldExpression("b"),
                                                                               Operation.ADD)),
                                           new Field("e", new BinaryExpression(new FieldExpression("b"),
                                                                               new FieldExpression("c"),
                                                                               Operation.MUL)),
                                           new Field("f", new BinaryExpression(new FieldExpression("d"),
                                                                               new FieldExpression("e"),
                                                                               Operation.ADD)),
                                           new Field("g", new FieldExpression("g")),
                                           new Field("h", new UnaryExpression(new FieldExpression("a"), Operation.SIZE_OF)));

        Projection projection = new Projection(fields);
        BulletRecord newRecord = projection.project(record, new TypedAvroBulletRecordProvider());

        Assert.assertEquals(newRecord.fieldCount(), 2);
        Assert.assertEquals(newRecord.typedGet("d").getValue(), 6);
        Assert.assertEquals(newRecord.typedGet("e").getValue(), 24);
    }

    @Test
    public void testProjectOldRecord() {
        BulletRecord record = RecordBox.get().add("a", 2)
                                             .add("b", 4)
                                             .add("c", 6).getRecord();

        List<Field> fields = Arrays.asList(new Field("a", new BinaryExpression(new FieldExpression("a"),
                                                                               new FieldExpression("b"),
                                                                               Operation.MUL)),
                                           new Field("b", new BinaryExpression(new FieldExpression("b"),
                                                                               new FieldExpression("c"),
                                                                               Operation.MUL)),
                                           new Field("d", new BinaryExpression(new FieldExpression("b"),
                                                                               new FieldExpression("c"),
                                                                               Operation.ADD)),
                                           new Field("e", new BinaryExpression(new FieldExpression("e"),
                                                                               new FieldExpression("f"),
                                                                               Operation.SUB)),
                                           new Field("f", new FieldExpression("f")),
                                           new Field("h", new UnaryExpression(new FieldExpression("a"), Operation.SIZE_OF)));

        Projection projection = new Projection(fields);
        BulletRecord oldRecord = projection.project(record);

        Assert.assertEquals(oldRecord.fieldCount(), 4);
        Assert.assertEquals(oldRecord.typedGet("a").getValue(), 8);
        Assert.assertEquals(oldRecord.typedGet("b").getValue(), 24);
        Assert.assertEquals(oldRecord.typedGet("c").getValue(), 6);
        Assert.assertEquals(oldRecord.typedGet("d").getValue(), 10);
        Assert.assertEquals(oldRecord, record);
    }
}
