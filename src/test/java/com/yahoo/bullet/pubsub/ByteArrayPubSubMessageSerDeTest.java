/*
 *  Copyright 2021 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.Raw;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ByteArrayPubSubMessageSerDeTest {
    @Test
    public void testConvertingQuery() {
        ByteArrayPubSubMessageSerDe serDe = new ByteArrayPubSubMessageSerDe(null);

        Query query = new Query(new Projection(), null, new Raw(1), null, new Window(), 1L);
        PubSubMessage actual = serDe.toMessage("id", query, "foo");
        Assert.assertEquals(actual.getId(), "id");
        Assert.assertEquals(actual.getContent(), SerializerDeserializer.toBytes(query));
        Assert.assertEquals(actual.getMetadata().getContent(), "foo");
    }

    @Test
    public void testDoNothingPubSubMessage() {
        ByteArrayPubSubMessageSerDe serDe = new ByteArrayPubSubMessageSerDe(null);
        PubSubMessage expected = new PubSubMessage("foo", new byte[0], new Metadata(Metadata.Signal.KILL, null));
        Assert.assertSame(serDe.toMessage(expected), expected);
        Assert.assertSame(serDe.fromMessage(expected), expected);
    }

    @Test
    public void testLazyMessage() {
        ByteArrayPubSubMessageSerDe serDe = new ByteArrayPubSubMessageSerDe(null);
        Query query = new Query(new Projection(), null, new Raw(1), null, new Window(), 1L);
        PubSubMessage converted = serDe.toMessage("id", query, "foo");
        PubSubMessage reverted = serDe.fromMessage(converted);

        Assert.assertSame(reverted, converted);

        // Starts off as byte[]
        Assert.assertEquals(reverted.getContent(), SerializerDeserializer.toBytes(query));

        // Payload is now made a Query
        Query revertedQuery = reverted.getContentAsQuery();
        Assert.assertEquals(revertedQuery.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(revertedQuery.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals((long) revertedQuery.getAggregation().getSize(), 1L);
        Assert.assertEquals((long) revertedQuery.getDuration(), 1L);
        Assert.assertSame(reverted.getContent(), revertedQuery);

        // Payload is now made a byte[]
        byte[] revertedByteArray = reverted.getContentAsByteArray();
        Assert.assertEquals(revertedByteArray, SerializerDeserializer.toBytes(query));
        Assert.assertSame(reverted.getContent(), revertedByteArray);
    }
}
