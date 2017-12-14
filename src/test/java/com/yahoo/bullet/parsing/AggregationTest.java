/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations;
import com.yahoo.bullet.operations.AggregationOperations.GroupOperationType;
import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.GroupAll;
import com.yahoo.bullet.aggregations.GroupBy;
import com.yahoo.bullet.aggregations.Raw;
import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.COUNT_DISTINCT;
import static com.yahoo.bullet.operations.AggregationOperations.AggregationType.GROUP;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.COUNT_FIELD;
import static com.yahoo.bullet.operations.AggregationOperations.GroupOperationType.SUM;
import static com.yahoo.bullet.parsing.AggregationUtils.makeAttributes;
import static com.yahoo.bullet.parsing.AggregationUtils.makeGroupOperation;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
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
        Assert.assertEquals(aggregation.getSize(), (Integer) 0);

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
        Assert.assertEquals(aggregation.getSize(), (Integer) 0);

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
        List<Error> errors = aggregation.initialize().get();
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
    public void testValidateNoField() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(makeAttributes(makeGroupOperation(SUM, null, null)));
        aggregation.configure(new BulletConfig());

        List<Error> errors = aggregation.initialize().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0).getError(), GroupOperation.GROUP_OPERATION_REQUIRES_FIELD + SUM);
    }

    @Test
    public void testUnsupportedOperation() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(makeAttributes(makeGroupOperation(COUNT_FIELD, "someField", "myCountField")));
        aggregation.configure(new BulletConfig());

        List<Error> errors = aggregation.initialize().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);
    }

    @Test
    public void testAttributeOperationMissing() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(singletonMap(GroupOperation.OPERATIONS, null));
        aggregation.configure(new BulletConfig());

        // Missing attribute operations should be silently validated
        List<Error> errors = aggregation.initialize().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);
    }

    @Test
    public void testAttributeOperationBadFormat() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(singletonMap(GroupOperation.OPERATIONS, asList("foo")));
        aggregation.configure(new BulletConfig());

        // Bad attribute operations should be silently validated
        List<Error> errors = aggregation.initialize().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);
    }

    @Test
    public void testAttributeOperationsUnknownOperation() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.setAttributes(makeAttributes(makeGroupOperation(COUNT, null, "bar"),
                                                 makeGroupOperation(COUNT_FIELD, "foo", "foo_avg")));
        aggregation.configure(new BulletConfig());

        // The bad operation should have been thrown out.
        Assert.assertEquals(aggregation.initialize(), Optional.<List<Error>>empty());
    }

    @Test
    public void testAttributeOperationsDuplicateOperation() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        // TODO Once other operations are supported, use them and not COUNT with fake fields as a proxy.
        aggregation.setAttributes(makeAttributes(makeGroupOperation(COUNT, null, null),
                makeGroupOperation(COUNT, "foo", null),
                makeGroupOperation(COUNT, null, null),
                makeGroupOperation(COUNT, "bar", null),
                makeGroupOperation(COUNT, "foo", null),
                makeGroupOperation(COUNT, "bar", null)));
        aggregation.configure(new BulletConfig());
        // The bad ones should be removed.
        Assert.assertEquals(aggregation.initialize(), Optional.<List<Error>>empty());
    }

    @Test
    public void testFailValidateOnCountDistinctFieldsMissing() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(COUNT_DISTINCT);
        aggregation.configure(new BulletConfig());

        List<Error> errors = aggregation.initialize().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Strategy.REQUIRES_FIELD_ERROR);
    }

    @Test
    public void testFailValidateOnGroupFieldsAndOperationsMissing() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(GROUP);
        aggregation.configure(new BulletConfig());

        List<Error> errors = aggregation.initialize().get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), GroupOperation.REQUIRES_FIELD_OR_OPERATION_ERROR);
    }

    @Test
    public void testToString() {
        Aggregation aggregation = new Aggregation();
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.toString(), "{size: 1, type: RAW, fields: null, attributes: null}");

        aggregation.setType(COUNT_DISTINCT);
        Assert.assertEquals(aggregation.toString(), "{size: 1, type: COUNT_DISTINCT, fields: null, attributes: null}");

        aggregation.setFields(singletonMap("field", "newName"));
        Assert.assertEquals(aggregation.toString(),
                "{size: 1, type: COUNT_DISTINCT, " + "fields: {field=newName}, attributes: null}");

        aggregation.setAttributes(singletonMap("foo", asList(1, 2, 3)));
        Assert.assertEquals(aggregation.toString(),
                "{size: 1, type: COUNT_DISTINCT, " + "fields: {field=newName}, attributes: {foo=[1, 2, 3]}}");
    }

    @Test
    public void testUnimplementedStrategies() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(null);
        aggregation.configure(new BulletConfig());

        Assert.assertNull(aggregation.getStrategy());
    }

    @Test
    public void testRawStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.findStrategy().getClass(), Raw.class);
    }

    @Test
    public void testGroupAllStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setAttributes(singletonMap(GroupOperation.OPERATIONS,
                                               singletonList(singletonMap(GroupOperation.OPERATION_TYPE,
                                                                          GroupOperationType.COUNT.getName()))));
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.getStrategy().getClass(), GroupAll.class);
    }

    @Test
    public void testCountDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.COUNT_DISTINCT);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.getStrategy().getClass(), CountDistinct.class);
    }

    @Test
    public void testDistinctStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.getStrategy().getClass(), GroupBy.class);
    }

    @Test
    public void testGroupByStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.GROUP);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.setAttributes(singletonMap(GroupOperation.OPERATIONS,
                                               singletonList(singletonMap(GroupOperation.OPERATION_TYPE,
                                                                          GroupOperationType.COUNT.getName()))));
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.getStrategy().getClass(), GroupBy.class);
    }

    @Test
    public void testDistributionStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.DISTRIBUTION);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.getStrategy().getClass(), Distribution.class);
    }

    @Test
    public void testTopKStrategy() {
        Aggregation aggregation = new Aggregation();
        aggregation.setType(AggregationOperations.AggregationType.TOP_K);
        aggregation.setFields(singletonMap("field", "foo"));
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.getStrategy().getClass(), TopK.class);
    }
}
