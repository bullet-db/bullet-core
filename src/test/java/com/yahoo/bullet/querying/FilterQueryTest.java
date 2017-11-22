/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.FilterOperations.FilterType;
import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.yahoo.bullet.TestHelpers.getListBytes;
import static com.yahoo.bullet.parsing.QueryUtils.getFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeProjectionFilterQuery;
import static com.yahoo.bullet.parsing.QueryUtils.makeRawFullQuery;

public class FilterQueryTest {
    private static Map<String, Object> configWithNoTimestamp() {
        return Collections.singletonMap(BulletConfig.RECORD_INJECT_TIMESTAMP, false);
    }

    @Test
    public void testFilteringProjection() {
        FilterQuery query = getFilterQuery(makeProjectionFilterQuery("map_field.id", Arrays.asList("1", "23"),
                                                                     FilterType.EQUALS, Pair.of("map_field.id", "mid")),
                                                                     configWithNoTimestamp());
        RecordBox boxA = RecordBox.get().addMap("map_field", Pair.of("id", "3"));
        Assert.assertFalse(query.consume(boxA.getRecord()));
        Assert.assertNull(query.getData());

        RecordBox boxB = RecordBox.get().addMap("map_field", Pair.of("id", "23"));
        RecordBox expected = RecordBox.get().add("mid", "23");
        Assert.assertTrue(query.consume(boxB.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expected.getRecord()));
    }

    @Test
    public void testNoAggregationAttempted() {
        FilterQuery query = getFilterQuery(makeRawFullQuery("map_field.id", Arrays.asList("1", "23"), FilterType.EQUALS,
                                                            AggregationType.RAW, BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE,
                                                            Pair.of("map_field.id", "mid")), configWithNoTimestamp());

        RecordBox boxA = RecordBox.get().addMap("map_field", Pair.of("id", "23"));
        RecordBox expectedA = RecordBox.get().add("mid", "23");
        Assert.assertTrue(query.consume(boxA.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expectedA.getRecord()));

        RecordBox boxB = RecordBox.get().addMap("map_field", Pair.of("id", "3"));
        Assert.assertFalse(query.consume(boxB.getRecord()));
        Assert.assertNull(query.getData());

        RecordBox boxC = RecordBox.get().addMap("map_field", Pair.of("id", "1"));
        RecordBox expectedC = RecordBox.get().add("mid", "1");
        Assert.assertTrue(query.consume(boxC.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expectedC.getRecord()));
    }

    @Test
    public void testMaximumEmitted() {
        FilterQuery query = getFilterQuery(makeAggregationQuery(AggregationType.RAW, 2), configWithNoTimestamp());
        RecordBox box = RecordBox.get();
        Assert.assertTrue(query.consume(box.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(box.getRecord()));
        Assert.assertTrue(query.consume(box.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(box.getRecord()));
        for (int i = 0; i < 10; ++i) {
            Assert.assertFalse(query.consume(box.getRecord()));
            Assert.assertNull(query.getData());
        }
    }

    @Test
    public void testMaximumEmittedWithNonMatchingRecords() {
        FilterQuery query = getFilterQuery(makeRawFullQuery("mid", Arrays.asList("1", "23"), FilterType.EQUALS,
                                                            AggregationType.RAW, 2, Pair.of("mid", "mid")),
                                           configWithNoTimestamp());
        RecordBox boxA = RecordBox.get().add("mid", "23");
        RecordBox expectedA = RecordBox.get().add("mid", "23");

        RecordBox boxB = RecordBox.get().add("mid", "42");

        Assert.assertTrue(query.consume(boxA.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expectedA.getRecord()));

        Assert.assertFalse(query.consume(boxB.getRecord()));
        Assert.assertNull(query.getData());

        Assert.assertFalse(query.consume(boxB.getRecord()));
        Assert.assertNull(query.getData());

        Assert.assertTrue(query.consume(boxA.getRecord()));
        Assert.assertEquals(query.getData(), getListBytes(expectedA.getRecord()));

        for (int i = 0; i < 10; ++i) {
            Assert.assertFalse(query.consume(boxA.getRecord()));
            Assert.assertNull(query.getData());
            Assert.assertFalse(query.consume(boxB.getRecord()));
            Assert.assertNull(query.getData());
        }
    }

    @Test(expectedExceptions = ParsingException.class)
    public void testValidationFail() throws ParsingException {
        new FilterQuery("{ 'aggregation': { 'type': null } }", new BulletConfig());
    }
}
