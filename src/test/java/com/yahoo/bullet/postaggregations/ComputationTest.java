/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ComputationTest {
    private Computation makeComputation(String expression, String newName) {
        PostAggregation postAggregation = new PostAggregation();
        postAggregation.setType(PostAggregation.Type.COMPUTATION);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("expression", expression);
        attributes.put("newName", newName);
        postAggregation.setAttributes(attributes);
        Computation computation = new Computation(postAggregation);
        computation.initialize();
        return computation;
    }

    @Test
    public void testInitializeWithoutAttributes() {
        PostAggregation postAggregation = new PostAggregation();
        postAggregation.setType(PostAggregation.Type.COMPUTATION);
        PostStrategy computation = new Computation(postAggregation);
        Optional<List<BulletError>> errors = computation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), Computation.COMPUTATION_REQUIRES_EXPRESSION_ERROR);
    }

    @Test
    public void testInitializeWithoutExpression() {
        PostAggregation postAggregation = new PostAggregation();
        postAggregation.setType(PostAggregation.Type.COMPUTATION);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("foo", "foo");
        postAggregation.setAttributes(attributes);
        PostStrategy computation = new Computation(postAggregation);
        Optional<List<BulletError>> errors = computation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), Computation.COMPUTATION_REQUIRES_EXPRESSION_ERROR);
    }

    @Test
    public void testInitializeWithoutNewField() {
        PostAggregation postAggregation = new PostAggregation();
        postAggregation.setType(PostAggregation.Type.COMPUTATION);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("expression", "foo");
        postAggregation.setAttributes(attributes);
        PostStrategy computation = new Computation(postAggregation);
        Optional<List<BulletError>> errors = computation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0), Computation.COMPUTATION_REQUIRES_NEW_FIELD_ERROR);
    }

    @Test
    public void testInitializeWithoutMalformedExpression() {
        PostAggregation postAggregation = new PostAggregation();
        postAggregation.setType(PostAggregation.Type.COMPUTATION);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("expression", "foo");
        attributes.put("newName", "bar");
        postAggregation.setAttributes(attributes);
        PostStrategy computation = new Computation(postAggregation);
        Optional<List<BulletError>> errors = computation.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertTrue(errors.get().get(0).getError().contains("The expression of the COMPUTATION post aggregation is invalid: java.lang.RuntimeException"));
    }

    @Test
    public void testInitialize() {
        PostAggregation postAggregation = new PostAggregation();
        postAggregation.setType(PostAggregation.Type.COMPUTATION);
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("expression", "1+2");
        attributes.put("newName", "bar");
        postAggregation.setAttributes(attributes);
        Computation computation = new Computation(postAggregation);
        Optional<List<BulletError>> errors = computation.initialize();
        Assert.assertFalse(errors.isPresent());
        Assert.assertEquals(computation.getExpression(), "1+2");
        Assert.assertEquals(computation.getNewFieldName(), "bar");
    }

    @Test
    public void testComputation() {
        Computation computation = makeComputation("(1 / 1.0e1) * 1.0e1 + cast(2.0, DOUBLE) * FIELD(a) - SIZEOF(FIELD(b))", "newName");
        List<BulletRecord> records = new ArrayList<>();
        records.add(RecordBox.get().add("a", 5).add("b", "").getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", "1").getRecord());
        records.add(RecordBox.get().add("a", 2).add("b", "11").getRecord());
        records.add(RecordBox.get().add("a", 1).add("b", "111").getRecord());
        Clip clip = new Clip();
        clip.add(records);
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), 11.0);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Double);
        Assert.assertEquals(result.getRecords().get(1).get("newName"), 4.0);
        Assert.assertEquals(result.getRecords().get(2).get("newName"), 3.0);
        Assert.assertEquals(result.getRecords().get(3).get("newName"), 0.0);
    }

    @Test
    public void testIntegerComputation() {
        Computation computation = makeComputation("cast(1, INTEGER) + cast(2, INTEGER) - cast(5, INTEGER) * cast(3, INTEGER) + (cast(1, INTEGER) / cast(1, INTEGER)) + (cast(2, INTEGER) -cast(1, INTEGER))", "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), -10);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Integer);
    }

    @Test
    public void testLongComputation() {
        Computation computation = makeComputation("1 + 2 - 5 * 3 + (1 / 1) + (2 - 2) + +2 + -2", "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), -11L);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Long);
    }

    @Test
    public void testFloatComputation() {
        Computation computation = makeComputation("cast(1, FLOAT) + cast(2, FLOAT) - cast(5, FLOAT) * cast(3, FLOAT) + (cast(1, FLOAT) / cast(1, FLOAT)) + (cast(2, FLOAT) -cast(1, FLOAT))", "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), -10.0f);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Float);
    }

    @Test
    public void testDoubleComputation() {
        Computation computation = makeComputation("1.0 + 2.0 - 5.0 * 3.0 + (1.0 / 1.0) + (2.0 - 2.0) + +2.0 + -2.0", "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), -11.0);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Double);
    }

    @Test
    public void testBooleanComputation() {
        Computation computation = makeComputation("true", "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), true);
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof Boolean);
    }

    @Test
    public void testStringComputation() {
        Computation computation = makeComputation("'abc'", "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), "abc");
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof String);
    }

    @Test
    public void testUnsupportedComputation() {
        Computation computation = makeComputation("1 + true", "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), "N/A");
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof String);
    }

    @Test
    public void testNonExistingFieldComputation() {
        Computation computation = makeComputation("FIELD(a)", "newName");
        Clip clip = new Clip();
        clip.add(Collections.singletonList(RecordBox.get().getRecord()));
        Clip result = computation.execute(clip);

        Assert.assertEquals(result.getRecords().get(0).get("newName"), "N/A");
        Assert.assertTrue(result.getRecords().get(0).get("newName") instanceof String);
    }
}
