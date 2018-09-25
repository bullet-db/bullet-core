/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryTest {
    @Test
    public void testDefaults() {
        Query query = new Query();
        BulletConfig config = new BulletConfig();
        query.configure(config);

        Assert.assertNull(query.getProjection());
        Assert.assertNull(query.getFilters());
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_QUERY_DURATION);
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
        Assert.assertEquals((Object) query.getAggregation().getSize(), BulletConfig.DEFAULT_AGGREGATION_SIZE);
    }

    @Test
    public void testAggregationForced() {
        Query query = new Query();
        query.setAggregation(null);
        Assert.assertNull(query.getProjection());
        Assert.assertNull(query.getFilters());
        // If you had null for aggregation
        Assert.assertNull(query.getAggregation());
        query.configure(new BulletConfig());
        Assert.assertNotNull(query.getAggregation());
    }

    @Test
    public void testAggregationDefault() {
        Query query = new Query();
        Aggregation aggregation = new Aggregation();
        aggregation.setType(null);
        aggregation.setSize(BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE - 1);
        query.setAggregation(aggregation);

        Assert.assertNull(aggregation.getType());
        query.configure(new BulletConfig());

        // Query no longer fixes type
        Assert.assertNull(aggregation.getType());
        Assert.assertEquals(aggregation.getSize(), new Integer(BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE - 1));
    }

    @Test
    public void testDuration() {
        BulletConfig config = new BulletConfig();

        Query query = new Query();
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_QUERY_DURATION);

        query.setDuration(-1000L);
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_QUERY_DURATION);

        query.setDuration(0L);
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_QUERY_DURATION);

        query.setDuration(1L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 1L);

        query.setDuration(BulletConfig.DEFAULT_QUERY_DURATION);
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_QUERY_DURATION);

        query.setDuration(BulletConfig.DEFAULT_QUERY_MAX_DURATION);
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_QUERY_MAX_DURATION);

        // Overflow
        query.setDuration(BulletConfig.DEFAULT_QUERY_MAX_DURATION * 2);
        query.configure(config);
        Assert.assertEquals((Object) query.getDuration(), BulletConfig.DEFAULT_QUERY_MAX_DURATION);
    }

    @Test
    public void testCustomDuration() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.QUERY_DEFAULT_DURATION, 200);
        config.set(BulletConfig.QUERY_MAX_DURATION, 1000);
        config.validate();

        Query query = new Query();

        query.setDuration(null);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 200L);

        query.setDuration(-1000L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 200L);

        query.setDuration(0L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 200L);

        query.setDuration(1L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 1L);

        query.setDuration(200L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 200L);

        query.setDuration(1000L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 1000L);

        query.setDuration(2000L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 1000L);
    }

    @Test
    public void testWindowForced() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        query.setWindow(WindowUtils.makeSlidingWindow(1));
        query.configure(config);
        Assert.assertNotNull(query.getWindow());

        config.set(BulletConfig.WINDOW_DISABLE, true);
        config.validate();
        query.setWindow(WindowUtils.makeSlidingWindow(1));
        query.configure(config);
        Assert.assertNull(query.getWindow());
    }

    @Test
    public void testInitialize() {
        Query query = new Query();
        Aggregation mockAggregation = mock(Aggregation.class);
        Optional<List<BulletError>> aggregationErrors = Optional.of(asList(new ParsingError("foo", new ArrayList<>()),
                                                                           new ParsingError("bar", new ArrayList<>())));
        when(mockAggregation.initialize()).thenReturn(aggregationErrors);
        query.setAggregation(mockAggregation);

        Clause mockClauseA = mock(Clause.class);
        Clause mockClauseB = mock(Clause.class);
        when(mockClauseA.initialize()).thenReturn(Optional.of(singletonList(new ParsingError("baz", new ArrayList<>()))));
        when(mockClauseB.initialize()).thenReturn(Optional.of(singletonList(new ParsingError("qux", new ArrayList<>()))));
        query.setFilters(asList(mockClauseA, mockClauseB));

        Projection mockProjection = mock(Projection.class);
        when(mockProjection.initialize()).thenReturn(Optional.of(singletonList(new ParsingError("quux", new ArrayList<>()))));
        query.setProjection(mockProjection);

        OrderBy orderByA = new OrderBy();
        orderByA.setType(PostAggregation.Type.ORDER_BY);
        orderByA.setFields(Collections.singletonList(new OrderBy.SortItem("a", OrderBy.Direction.ASC)));
        OrderBy orderByB = new OrderBy();
        orderByB.setType(PostAggregation.Type.ORDER_BY);
        orderByB.setFields(Collections.singletonList(new OrderBy.SortItem("a", OrderBy.Direction.ASC)));
        query.setPostAggregations(Arrays.asList(orderByA, orderByB));

        Optional<List<BulletError>> errorList = query.initialize();
        Assert.assertTrue(errorList.isPresent());
        Assert.assertEquals(errorList.get().size(), 6);
    }

    @Test
    public void testInitializeNullValues() {
        Query query = new Query();
        query.setProjection(null);
        query.setFilters(null);
        query.setAggregation(null);
        query.configure(new BulletConfig());
        Optional<List<BulletError>> errorList = query.initialize();
        Assert.assertFalse(errorList.isPresent());
    }

    @Test
    public void testToString() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.AGGREGATION_DEFAULT_SIZE, 1);
        config.set(BulletConfig.QUERY_DEFAULT_DURATION, 30000L);
        Query query = new Query();
        query.configure(config.validate());

        Assert.assertEquals(query.toString(),
                "{" +
                "filters: null, projection: null, " +
                "aggregation: {size: 1, type: RAW, fields: null, attributes: null}, " +
                "postAggregations: null, " +
                "window: null, " +
                "duration: 30000" +
                "}");

        query.setFilters(singletonList(FilterUtils.getFieldFilter(Clause.Operation.EQUALS, "foo", "bar")));
        query.setProjection(ProjectionUtils.makeProjection("field", "bid"));
        query.configure(config);

        Assert.assertEquals(query.toString(),
                            "{" +
                            "filters: [{operation: EQUALS, field: field, values: [foo, bar]}], " +
                            "projection: {fields: {field=bid}}, " +
                            "aggregation: {size: 1, type: RAW, fields: null, attributes: null}, " +
                            "postAggregations: null, " +
                            "window: null, " +
                            "duration: 30000" +
                            "}");

        query.setWindow(WindowUtils.makeTumblingWindow(4000));
        query.configure(config);
        Assert.assertEquals(query.toString(),
                            "{" +
                            "filters: [{operation: EQUALS, field: field, values: [foo, bar]}], " +
                            "projection: {fields: {field=bid}}, " +
                            "aggregation: {size: 1, type: RAW, fields: null, attributes: null}, " +
                            "postAggregations: null, " +
                            "window: {emit: {type=TIME, every=4000}, include: null}, " +
                            "duration: 30000" +
                            "}");
    }
}
