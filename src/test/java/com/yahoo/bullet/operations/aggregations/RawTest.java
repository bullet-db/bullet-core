/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.TestHelpers;
import com.yahoo.bullet.operations.SerializerDeserializer;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.getListBytes;
import static java.util.stream.Collectors.toList;

public class RawTest {
    private class NoSerDeBulletRecord extends BulletRecord implements Serializable {
        private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            throw new IOException("Forced test serialization failure");
        }

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            throw new IOException("Forced test deserialization failure");
        }
    }

    public static Raw makeRaw(int size, int microBatchSize, int maxSize) {
        Aggregation aggregation = new Aggregation();
        aggregation.setSize(size);
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RAW_AGGREGATION_MAX_SIZE, maxSize);
        config.set(BulletConfig.RAW_AGGREGATION_MICRO_BATCH_SIZE, microBatchSize);
        aggregation.setConfiguration(config.validate());
        Raw raw = new Raw(aggregation);
        raw.initialize();
        return raw;
    }

    public static Raw makeRaw(int size, int microBatchSize) {
        return makeRaw(size, microBatchSize, BulletConfig.DEFAULT_RAW_AGGREGATION_MAX_SIZE);
    }

    public static Raw makeRaw(int size) {
        return makeRaw(size, 1);
    }

    @Test
    public void testInitialize() {
        Assert.assertNull(makeRaw(20, 2, 15).initialize());
    }

    @Test
    public void testCanAcceptData() {
        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        Raw raw = makeRaw(2);

        Assert.assertTrue(raw.isAcceptingData());

        raw.consume(record);
        Assert.assertTrue(raw.isAcceptingData());

        raw.consume(record);
        Assert.assertFalse(raw.isAcceptingData());
    }

    @Test
    public void testDefaultMicroBatching() {
        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        Raw raw = makeRaw(2);

        Assert.assertFalse(raw.isMicroBatch());
        raw.consume(record);
        Assert.assertTrue(raw.isMicroBatch());
        Assert.assertNotNull(raw.getSerializedAggregation());
        Assert.assertFalse(raw.isMicroBatch());
        raw.consume(record);
        Assert.assertTrue(raw.isMicroBatch());
    }

    @Test
    public void testCustomMicroBatching() {
        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        Raw raw = makeRaw(3, 2);
        Assert.assertFalse(raw.isMicroBatch());
        raw.consume(record);
        Assert.assertFalse(raw.isMicroBatch());
        raw.consume(record);
        Assert.assertTrue(raw.isMicroBatch());
        Assert.assertNotNull(raw.getSerializedAggregation());
        Assert.assertFalse(raw.isMicroBatch());
        raw.consume(record);
        Assert.assertFalse(raw.isMicroBatch());
    }

    @Test
    public void testNeverMicroBatching() {
        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        Raw raw = makeRaw(1, 2);
        Assert.assertFalse(raw.isMicroBatch());
        Assert.assertTrue(raw.isAcceptingData());
        raw.consume(record);
        // Not a micro-batch even though size has been reached
        Assert.assertFalse(raw.isMicroBatch());
        Assert.assertFalse(raw.isAcceptingData());
        // But you can still get the < micro-batch
        Assert.assertNotNull(raw.getSerializedAggregation());
    }

    @Test
    public void testNull() {
        Raw raw = makeRaw(1);
        raw.consume(null);
        Assert.assertNull(raw.getSerializedAggregation());
        raw.combine(null);
        Assert.assertNull(raw.getSerializedAggregation());
        Assert.assertEquals(raw.getAggregation().getRecords().size(), 0);
    }

    @Test
    public void testWritingBadRecord() throws IOException {
        BulletRecord mocked = new NoSerDeBulletRecord();

        Raw raw = makeRaw(1);
        raw.consume(mocked);
        Assert.assertNull(raw.getSerializedAggregation());
    }

    @Test
    public void testReadingBadSerialization() throws IOException {
        Raw raw = makeRaw(1);
        raw.combine(new byte[0]);

        Assert.assertNull(raw.getSerializedAggregation());
    }

    @Test
    public void testReadingEmpty() throws IOException {
        Raw raw = makeRaw(1);
        raw.combine(SerializerDeserializer.toBytes(new ArrayList<>()));
        Assert.assertNull(raw.getSerializedAggregation());
    }

    @Test
    public void testSerializationOnConsumedRecord() {
        Raw raw = makeRaw(2);

        BulletRecord recordA = RecordBox.get().add("foo", "bar").getRecord();
        raw.consume(recordA);

        Assert.assertEquals(raw.getSerializedAggregation(), getListBytes(recordA));

        BulletRecord recordB = RecordBox.get().add("bar", "baz").getRecord();
        raw.consume(recordB);

        Assert.assertEquals(raw.getSerializedAggregation(), getListBytes(recordB));

        Assert.assertFalse(raw.isAcceptingData());

        BulletRecord recordC = RecordBox.get().add("baz", "qux").getRecord();
        // This consumption should not occur
        raw.consume(recordC);
        Assert.assertNull(raw.getSerializedAggregation());
    }

    @Test
    public void testLimitZero() {
        Raw raw = makeRaw(0);

        List<BulletRecord> aggregate = raw.getAggregation().getRecords();
        Assert.assertFalse(raw.isAcceptingData());
        Assert.assertFalse(raw.isMicroBatch());
        Assert.assertEquals(aggregate.size(), 0);

        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        raw.consume(record);

        Assert.assertFalse(raw.isAcceptingData());
        Assert.assertFalse(raw.isMicroBatch());
        Assert.assertNull(raw.getSerializedAggregation());
        Assert.assertEquals(raw.getAggregation().getRecords().size(), 0);
    }

    @Test
    public void testLimitLessThanSpecified() {
        Raw raw = makeRaw(10);
        List<BulletRecord> records = IntStream.range(0, 5).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(toList());

        records.stream().map(TestHelpers::getListBytes).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getAggregation().getRecords();
        // We should have 5 records
        Assert.assertEquals(aggregate.size(), 5);
        // We should have all the records
        Assert.assertEquals(aggregate, records);
    }

    @Test
    public void testLimitExact() {
        Raw raw = makeRaw(10);

        List<BulletRecord> records = IntStream.range(0, 10).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(toList());

        records.stream().map(TestHelpers::getListBytes).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getAggregation().getRecords();
        // We should have 10 records
        Assert.assertEquals(aggregate.size(), 10);
        // We should have the all records
        Assert.assertEquals(aggregate, records);
    }


    @Test
    public void testLimitMoreThanMaximum() {
        Raw raw = makeRaw(10);

        List<BulletRecord> records = IntStream.range(0, 20).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(toList());

        records.stream().map(TestHelpers::getListBytes).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getAggregation().getRecords();
        // We should have 10 records
        Assert.assertEquals(aggregate.size(), 10);
        // We should have the first 10 records
        Assert.assertEquals(aggregate, records.subList(0, 10));
    }

    @Test
    public void testLimitDifferentMicroBatches() {
        Raw raw = makeRaw(20);
        List<BulletRecord> records = IntStream.range(0, 20).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(Collectors.toCollection(ArrayList::new));

        byte[] batchOfOne = getListBytes(records.subList(1, 2).toArray(new BulletRecord[1]));
        byte[] batchOfThree = getListBytes(records.subList(2, 5).toArray(new BulletRecord[3]));
        byte[] batchOfFive = getListBytes(records.subList(5, 10).toArray(new BulletRecord[5]));
        byte[] batchOfTen = getListBytes(records.subList(10, 20).toArray(new BulletRecord[10]));

        byte[] extraTen = getListBytes((BulletRecord[]) records.subList(0, 10).toArray(new BulletRecord[10]));

        raw.combine(batchOfOne);
        raw.combine(batchOfThree);
        raw.combine(batchOfFive);
        raw.combine(batchOfTen);
        // We should have 19 records at this point, and the next consume should just take the first one in the batch
        raw.combine(extraTen);

        List<BulletRecord> actual = raw.getAggregation().getRecords();
        Assert.assertEquals(actual.size(), 20);

        // We should have 1, 2, ... 19, 0 as the records
        List<BulletRecord> expected = IntStream.range(1, 20).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                               .collect(Collectors.toCollection(ArrayList::new));
        expected.add(RecordBox.get().add("i", 0).getRecord());
        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testLimitConfiguredMaximums() {
        Raw raw = makeRaw(50000, 1, 200);
        List<BulletRecord> records = IntStream.range(0, 300).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(toList());
        records.stream().map(TestHelpers::getListBytes).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getAggregation().getRecords();
        Assert.assertEquals(aggregate.size(), 200);
        Assert.assertEquals(aggregate, records.subList(0, 200));
    }
}
