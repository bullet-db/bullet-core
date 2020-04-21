/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.result;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.result.Meta.Concept;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class MetaTest {
    @Test
    public void testConceptFinding() {
        Assert.assertEquals(Concept.from("Query Receive Time"), Concept.QUERY_RECEIVE_TIME);
        Assert.assertEquals(Concept.from("Query ID"), Concept.QUERY_ID);
        Assert.assertEquals(Concept.from("Sketch Metadata"), Concept.SKETCH_METADATA);
        Assert.assertEquals(Concept.from("Sketch Standard Deviations"), Concept.SKETCH_STANDARD_DEVIATIONS);
        Assert.assertNull(Concept.from("foo"));
        Assert.assertNull(Concept.from("sketch standard deviations"));
    }

    @Test
    public void testMetadataWithErrors() {
        BulletError errorA = new BulletError("foo", "bar");
        BulletError errorB = new BulletError("baz", "qux");
        Meta meta = Meta.of(Arrays.asList(errorA, errorB));
        BulletError errorC = new BulletError("norf", "foo");
        meta.addErrors(Collections.singletonList(errorC));

        Map<String, Object> actual = meta.asMap();
        Assert.assertEquals(actual.size(), 1);
        List<BulletError> actualErrors = (List<BulletError>) actual.get(Meta.ERROR_KEY);
        Assert.assertEquals(actualErrors.size(), 3);
        Assert.assertEquals(actualErrors.get(0).getError(), "foo");
        Assert.assertEquals(actualErrors.get(0).getResolutions(), singletonList("bar"));
        Assert.assertEquals(actualErrors.get(1).getError(), "baz");
        Assert.assertEquals(actualErrors.get(1).getResolutions(), singletonList("qux"));
        Assert.assertEquals(actualErrors.get(2).getError(), "norf");
        Assert.assertEquals(actualErrors.get(2).getResolutions(), singletonList("foo"));
    }

    @Test
    public void testOfVarargs() {
        BulletError errorA = new BulletError("foo", "bar");
        BulletError errorB = new BulletError("baz", "qux");
        Meta meta = Meta.of(errorA, errorB);

        Map<String, Object> actual = meta.asMap();
        Assert.assertEquals(actual.size(), 1);
        List<BulletError> actualErrors = (List<BulletError>) actual.get(Meta.ERROR_KEY);
        Assert.assertEquals(actualErrors.size(), 2);
        Assert.assertEquals(actualErrors.get(0).getError(), "foo");
        Assert.assertEquals(actualErrors.get(0).getResolutions(), singletonList("bar"));
        Assert.assertEquals(actualErrors.get(1).getError(), "baz");
        Assert.assertEquals(actualErrors.get(1).getResolutions(), singletonList("qux"));
    }

    @Test
    public void testMerging() {
        Meta metaA = new Meta();
        metaA.add("foo", singletonList("bar"));
        metaA.add("bar", 1L);
        Meta metaB = new Meta();
        metaB.add("baz", singletonMap("a", 1));
        metaB.add("bar", 0.3);
        Meta metaC = null;

        metaA.merge(metaB);
        metaA.merge(metaC);

        Map<String, Object> expected = new HashMap<>();
        expected.put("foo", singletonList("bar"));
        expected.put("bar", 0.3);
        expected.put("baz", singletonMap("a", 1));

        Assert.assertEquals(metaA.asMap(), expected);
    }

    @Test
    public void testConsumingRegisteredConcepts() {
        List<String> names = new ArrayList<>();

        Meta.consumeRegisteredConcept(Concept.QUERY_ID, singletonMap(Concept.QUERY_ID.getName(), "foo"), names::add);
        Assert.assertEquals(names.size(), 1);
        Assert.assertEquals(names.get(0), "foo");

        Meta.consumeRegisteredConcept(Concept.QUERY_ID, emptyMap(), names::add);
        Assert.assertEquals(names.size(), 1);
    }
}
