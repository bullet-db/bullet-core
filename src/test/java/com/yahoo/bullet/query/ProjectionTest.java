/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.query.expressions.ValueExpression;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

@SuppressWarnings("unchecked")
public class ProjectionTest {
    @Test
    public void testDefault() {
        Projection projection = new Projection();
        Assert.assertNull(projection.getFields());
        Assert.assertEquals(projection.getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(projection.toString(), "{fields: null, type: PASS_THROUGH}");
    }

    @Test
    public void testProjectionCopy() {
        Projection projection = new Projection(Arrays.asList(new Field("foo", new ValueExpression(5))), true);
        Assert.assertEquals(projection.getFields(), Collections.singletonList(new Field("foo", new ValueExpression(5))));
        Assert.assertEquals(projection.getType(), Projection.Type.COPY);
        Assert.assertEquals(projection.toString(), "{fields: [{name: foo, value: {value: 5, type: INTEGER}}], type: COPY}");
    }

    @Test
    public void testProjectionNoCopy() {
        Projection projection = new Projection(Arrays.asList(new Field("foo", new ValueExpression(5))), false);
        Assert.assertEquals(projection.getFields(), Collections.singletonList(new Field("foo", new ValueExpression(5))));
        Assert.assertEquals(projection.getType(), Projection.Type.NO_COPY);
        Assert.assertEquals(projection.toString(), "{fields: [{name: foo, value: {value: 5, type: INTEGER}}], type: NO_COPY}");
    }
}
