package com.yahoo.bullet.pubsub;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class MockSubscriber implements Subscriber {
    String commitID;
    String failID;

    int commitSequence;
    int failSequence;

    public PubSubMessage receive() {
        throw new UnsupportedOperationException();
    }

    public void commit(String id, int sequence) {
        commitID = id;
        commitSequence = sequence;
    }

    public void fail(String id, int sequence) {
        failID = id;
        failSequence = sequence;
    }

    public void close() {
        throw new UnsupportedOperationException();
    }
}
