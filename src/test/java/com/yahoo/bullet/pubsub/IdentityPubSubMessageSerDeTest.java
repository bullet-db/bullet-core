/*
 *  Copyright 2021 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.Raw;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IdentityPubSubMessageSerDeTest {
    @Test
    public void testConvertingQuery() {
        IdentityPubSubMessageSerDe serDe = new IdentityPubSubMessageSerDe(null);

        Query query = new Query(new Projection(), null, new Raw(1), null, new Window(), 1L);
        PubSubMessage actual = serDe.toMessage("id", query, "foo");
        Assert.assertEquals(actual.getId(), "id");
        Assert.assertSame(actual.getContent(), query);
        Assert.assertEquals(actual.getMetadata().getContent(), "foo");
    }

    @Test
    public void testConverting() {
        IdentityPubSubMessageSerDe serDe = new IdentityPubSubMessageSerDe(null);
        PubSubMessage expected = new PubSubMessage("foo", new byte[0], new Metadata(Metadata.Signal.CUSTOM, new byte[0]));
        Assert.assertSame(serDe.toMessage(expected), expected);
    }

    @Test
    public void testReverting() {
        IdentityPubSubMessageSerDe serDe = new IdentityPubSubMessageSerDe(null);
        PubSubMessage expected = new PubSubMessage("foo", new byte[0], new Metadata(Metadata.Signal.CUSTOM, new byte[0]));
        Assert.assertSame(serDe.fromMessage(expected), expected);
    }

    @Test
    public void testFrom() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.PUBSUB_MESSAGE_SERDE_CLASS_NAME, IdentityPubSubMessageSerDe.class.getName());
        PubSubMessageSerDe manager = PubSubMessageSerDe.from(config);
        Assert.assertTrue(manager instanceof IdentityPubSubMessageSerDe);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Cannot create.*")
    public void testFromWithABadClass() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.PUBSUB_MESSAGE_SERDE_CLASS_NAME, "does.not.exist");
        PubSubMessageSerDe.from(config);
    }
}
