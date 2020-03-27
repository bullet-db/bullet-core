/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query;

import static org.mockito.Mockito.mock;

public class QueryTest {
    /*
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
                            "filters: [{operation: EQUALS, field: field, values: [{kind: VALUE, value: foo, type: null}, {kind: VALUE, value: bar, type: null}]}], " +
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
                            "filters: [{operation: EQUALS, field: field, values: [{kind: VALUE, value: foo, type: null}, {kind: VALUE, value: bar, type: null}]}], " +
                            "projection: {fields: {field=bid}}, " +
                            "aggregation: {size: 1, type: RAW, fields: null, attributes: null}, " +
                            "postAggregations: null, " +
                            "window: {emit: {type=TIME, every=4000}, include: null}, " +
                            "duration: 30000" +
                            "}");
    }

    @Test
    public void testRewritingClauses() {
        Query query = new Query();
        LogicalClause and = new LogicalClause();
        and.setOperation(Clause.Operation.AND);

        LogicalClause or = new LogicalClause();
        or.setOperation(Clause.Operation.OR);
        StringFilterClause equals = new StringFilterClause();
        equals.setOperation(Clause.Operation.EQUALS);
        equals.setField("A");
        equals.setValues(singletonList(Type.NULL_EXPRESSION));
        and.setClauses(asList(or, equals));

        query.setFilters(singletonList(and));
        query.configure(new BulletConfig());

        List<Clause> filters = query.getFilters();
        Assert.assertEquals(filters.size(), 1);
        LogicalClause rewrittenAnd = (LogicalClause) filters.get(0);
        Assert.assertEquals(rewrittenAnd.getClauses().size(), 2);

        LogicalClause rewrittenOr = (LogicalClause) rewrittenAnd.getClauses().get(0);
        Assert.assertNull(rewrittenOr.getClauses());
        Assert.assertEquals(rewrittenOr.getOperation(), Clause.Operation.OR);

        Clause rewrittenStringFilterClause = rewrittenAnd.getClauses().get(1);
        Assert.assertTrue(rewrittenStringFilterClause instanceof ObjectFilterClause);
        ObjectFilterClause rewritten = (ObjectFilterClause) rewrittenStringFilterClause;
        Assert.assertEquals(rewritten.getField(), "A");
        Assert.assertEquals(rewritten.getOperation(), Clause.Operation.EQUALS);
        Assert.assertEquals(rewritten.getValues().size(), 1);
        Value expectedValue = new Value(Value.Kind.VALUE, Type.NULL_EXPRESSION, null);
        Value actualValue = rewritten.getValues().get(0);
        Assert.assertEquals(actualValue.getType(), expectedValue.getType());
        Assert.assertEquals(actualValue.getValue(), expectedValue.getValue());
        Assert.assertEquals(actualValue.getKind(), expectedValue.getKind());
    }
    */
}
