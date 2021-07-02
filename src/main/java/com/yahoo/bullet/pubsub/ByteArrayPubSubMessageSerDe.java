/*
 *  Copyright 2021 Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.query.Query;

import java.io.Serializable;

/**
 * This SerDe is used to convert a provided {@link Query} into its serialized bytes in the {@link PubSubMessage}. When
 * invoking the {@link PubSubMessage#getContentAsByteArray()} or {@link PubSubMessage#getContentAsQuery()}, the payload
 * is lazily converted (and stored) as the appropriate type. This means that {@link PubSubMessage#getContent()} will
 * return a byte[] or {@link Query} depending on what was called before it. This can be used to send a {@link Query}
 * to the backend and if it is passed between multiple workers (i.e. serialized and deserialized multiple times), it
 * will not needlessly convert the {@link Query} object multiple times.
 *
 * This behaves like the {@link IdentityPubSubMessageSerDe} for all other operations.
 */
public class ByteArrayPubSubMessageSerDe extends IdentityPubSubMessageSerDe {
    private static final long serialVersionUID = -7648403271773714704L;

    /**
     * A {@link PubSubMessage} that is sticky for converting the content between byte[] and {@link Query}.
     */
    private static class LazyPubSubMessage extends PubSubMessage {
        private static final long serialVersionUID = -6516915913438279870L;

        private LazyPubSubMessage(String id, byte[] content, Metadata metadata) {
            super(id, content, metadata);
        }

        @Override
        public byte[] getContentAsByteArray() {
            if (content instanceof Query) {
                content = SerializerDeserializer.toBytes((Serializable) content);
            }
            return (byte[]) content;
        }

        @Override
        public Query getContentAsQuery() {
            if (content instanceof byte[]) {
                content = SerializerDeserializer.fromBytes((byte[]) content);
            }
            return (Query) content;
        }
    }

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to configure this class.
     */
    public ByteArrayPubSubMessageSerDe(BulletConfig config) {
        super(config);
    }

    @Override
    public PubSubMessage toMessage(String id, Query query, String queryString) {
        return toMessage(new LazyPubSubMessage(id, SerializerDeserializer.toBytes(query), new Metadata(null, queryString)));
    }
}
