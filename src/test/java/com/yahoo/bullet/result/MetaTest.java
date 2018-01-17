/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.result;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.ParsingError;
import com.yahoo.bullet.result.Meta.Concept;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class MetaTest {
    @SafeVarargs
    public static List<Map<String, String>> asMetadataEntries(Map.Entry<String, String>... pairs) {
        List<Map<String, String>> list = new ArrayList<>();
        for (Map.Entry<String, String> pair : pairs) {
            Map<String, String> map = new HashMap<>();
            map.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, pair.getKey());
            map.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, pair.getValue());
            list.add(map);
        }
        return list;
    }

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
    public void testErrorsAddition() {
        ParsingError errorA = new ParsingError("foo", asList("1", "2"));
        ParsingError errorB = new ParsingError("bar", asList("3", "4"));
        Meta meta = Meta.of(errorA, errorB);

        Map<String, Object> actual = meta.asMap();
        Assert.assertEquals(actual.size(), 1);

        List<ParsingError> actualErrors = (List<ParsingError>) actual.get(Meta.ERROR_KEY);
        Assert.assertEquals(actualErrors.size(), 2);
        Assert.assertEquals(actualErrors.get(0).getError(), "foo");
        Assert.assertEquals(actualErrors.get(0).getResolutions(), asList("1", "2"));
        Assert.assertEquals(actualErrors.get(1).getError(), "bar");
        Assert.assertEquals(actualErrors.get(1).getResolutions(), asList("3", "4"));

        ParsingError errorC = new ParsingError("baz", asList("5", "6"));
        ParsingError errorD = new ParsingError("qux", singletonList("7"));
        meta.addErrors(Arrays.asList(errorC, errorD));

        Assert.assertEquals(actualErrors.size(), 4);
        Assert.assertEquals(actualErrors.get(0).getError(), "foo");
        Assert.assertEquals(actualErrors.get(0).getResolutions(), asList("1", "2"));
        Assert.assertEquals(actualErrors.get(1).getError(), "bar");
        Assert.assertEquals(actualErrors.get(1).getResolutions(), asList("3", "4"));
        Assert.assertEquals(actualErrors.get(2).getError(), "baz");
        Assert.assertEquals(actualErrors.get(2).getResolutions(), asList("5", "6"));
        Assert.assertEquals(actualErrors.get(3).getError(), "qux");
        Assert.assertEquals(actualErrors.get(3).getResolutions(), singletonList("7"));
    }

    @Test
    public void testMetadataWithErrors() {
        BulletError errorA = BulletError.makeError("foo", "bar");
        BulletError errorB = BulletError.makeError("baz", "qux");
        Meta meta = Meta.of(Arrays.asList(errorA, errorB));
        BulletError errorC = BulletError.makeError("norf", "foo");
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
}
