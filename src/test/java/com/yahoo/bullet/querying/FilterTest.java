/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FilterTest {
    @Test
    public void testFilterMatch() {
        Filter filter = new Filter(new BinaryExpression(new FieldExpression("abc"), new ValueExpression(0), Operation.GREATER_THAN));

        BulletRecord recordA = RecordBox.get().add("abc", 1).getRecord();
        BulletRecord recordB = RecordBox.get().add("abc", 0).getRecord();
        BulletRecord recordC = RecordBox.get().getRecord();

        Assert.assertTrue(filter.match(recordA));
        Assert.assertFalse(filter.match(recordB));
        Assert.assertFalse(filter.match(recordC));
    }

    @Test
    public void testFilterMatchException() {
        Filter filter = new Filter(new FieldExpression("abc"));

        Assert.assertFalse(filter.match(null));
    }
}
