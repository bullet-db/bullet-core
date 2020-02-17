/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.AggregationUtils.makeGroupOperation;
import static com.yahoo.bullet.parsing.Aggregation.Type.COUNT_DISTINCT;
import static com.yahoo.bullet.parsing.Aggregation.Type.GROUP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

public class AggregationTest {
    @Test
    public void testSize() {
        Aggregation aggregation = new Aggregation();
        BulletConfig config = new BulletConfig();

        aggregation.setSize(null);
        aggregation.configure(config);
        Assert.assertEquals((Object) aggregation.getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);

        aggregation.setSize(-10);
        aggregation.configure(config);
        Assert.assertEquals((Object) aggregation.getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);

        aggregation.setSize(0);
        aggregation.configure(config);
        Assert.assertEquals((Object) aggregation.getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);

        aggregation.setSize(1);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 1);

        aggregation.setSize(BulletConfig.DEFAULT_AGGREGATION_SIZE);
        aggregation.configure(config);
        Assert.assertEquals((Object) aggregation.getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);

        aggregation.setSize(BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE + 1);
        aggregation.configure(config);
        Assert.assertEquals((Object) aggregation.getSize(), BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE);
    }

    @Test
    public void testConfiguredSize() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.AGGREGATION_DEFAULT_SIZE, 10);
        config.set(BulletConfig.AGGREGATION_MAX_SIZE, 200);
        // Need to lower or else AGGREGATION_MAX_SIZE will default to DEFAULT_AGGREGATION_MAX_SIZE
        config.set(BulletConfig.GROUP_AGGREGATION_MAX_SIZE, 200);
        config.validate();

        Aggregation aggregation = new Aggregation();

        aggregation.setSize(null);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation.setSize(-10);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation.setSize(0);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation.setSize(1);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 1);

        aggregation.setSize(10);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation.setSize(200);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 200);

        aggregation.setSize(4000);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 200);
    }

    @Test
    public void testFailValidateOnNoType() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(null);
        Optional<List<BulletError>> optionalErrors = aggregation.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Aggregation.TYPE_NOT_SUPPORTED_ERROR);
    }

    @Test
    public void testSuccessfulValidate() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(makeAttributes(makeGroupOperation(COUNT, null, "count")));
        aggregation.configure(new BulletConfig());

        Assert.assertFalse(aggregation.initialize().isPresent());
    }

    @Test
    public void testToString() {
        Aggregation aggregation = new Aggregation();
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.toString(), "{size: 500, type: RAW, fields: null, attributes: null}");

        aggregation.setType(COUNT_DISTINCT);
        Assert.assertEquals(aggregation.toString(), "{size: 500, type: COUNT_DISTINCT, fields: null, attributes: null}");

        aggregation.setFields(singletonMap("field", "newName"));
        Assert.assertEquals(aggregation.toString(),
                "{size: 500, type: COUNT_DISTINCT, " + "fields: {field=newName}, attributes: null}");

        aggregation.setAttributes(singletonMap("foo", asList(1, 2, 3)));
        Assert.assertEquals(aggregation.toString(),
                "{size: 500, type: COUNT_DISTINCT, " + "fields: {field=newName}, attributes: {foo=[1, 2, 3]}}");
    }
}
