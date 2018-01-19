/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations;

import com.yahoo.bullet.TestHelpers;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.TestHelpers.getListBytes;
import static java.util.stream.Collectors.toList;

public class RawTest {
    private static Raw makeRaw(int size, int maxSize) {
        Aggregation aggregation = new Aggregation();
        aggregation.setSize(size);
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RAW_AGGREGATION_MAX_SIZE, maxSize);
        Raw raw = new Raw(aggregation, config.validate());
        raw.initialize();
        return raw;
    }

    private static Raw makeRaw(int size) {
        return makeRaw(size, BulletConfig.DEFAULT_RAW_AGGREGATION_MAX_SIZE);
    }

    @Test
    public void testInitialize() {
        Assert.assertFalse(makeRaw(20, 15).initialize().isPresent());
    }

    @Test
    public void testIsClosed() {
        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        Raw raw = makeRaw(2);

        Assert.assertFalse(raw.isClosed());

        raw.consume(record);
        Assert.assertFalse(raw.isClosed());

        raw.consume(record);
        Assert.assertTrue(raw.isClosed());
    }

    @Test
    public void testNull() {
        Raw raw = makeRaw(1);
        raw.consume(null);
        Assert.assertNull(raw.getData());
        raw.combine(null);
        Assert.assertNull(raw.getData());
        Assert.assertEquals(raw.getResult().getRecords().size(), 0);
    }

    @Test
    public void testWritingBadRecord() {
        BulletRecord mocked = new NoSerDeBulletRecord();

        Raw raw = makeRaw(1);
        raw.consume(mocked);
        Assert.assertNull(raw.getData());
    }

    @Test
    public void testReadingBadSerialization() {
        Raw raw = makeRaw(1);
        raw.combine(new byte[0]);

        Assert.assertNull(raw.getData());
    }

    @Test
    public void testReadingEmpty() {
        Raw raw = makeRaw(1);
        raw.combine(SerializerDeserializer.toBytes(new ArrayList<>()));
        Assert.assertNull(raw.getData());
    }

    @Test
    public void testSerializationOnConsumedRecord() {
        Raw raw = makeRaw(2);

        BulletRecord recordA = RecordBox.get().add("foo", "bar").getRecord();
        raw.consume(recordA);

        Assert.assertEquals(raw.getData(), getListBytes(recordA));

        BulletRecord recordB = RecordBox.get().add("bar", "baz").getRecord();
        raw.consume(recordB);

        Assert.assertEquals(raw.getData(), getListBytes(recordA, recordB));

        Assert.assertTrue(raw.isClosed());

        BulletRecord recordC = RecordBox.get().add("baz", "qux").getRecord();

        // This consumption should not occur
        raw.consume(recordC);
        Assert.assertEquals(raw.getData(), getListBytes(recordA, recordB));

        raw.reset();
        Assert.assertNull(raw.getData());
    }

    @Test
    public void testLimitZero() {
        Raw raw = makeRaw(0);

        List<BulletRecord> aggregate = raw.getResult().getRecords();
        Assert.assertTrue(raw.isClosed());
        Assert.assertEquals(aggregate.size(), 0);

        BulletRecord record = RecordBox.get().add("foo", "bar").getRecord();
        raw.consume(record);

        Assert.assertTrue(raw.isClosed());
        Assert.assertNull(raw.getData());
        Assert.assertEquals(raw.getResult().getRecords().size(), 0);
    }

    @Test
    public void testLimitLessThanSpecified() {
        Raw raw = makeRaw(10);
        List<BulletRecord> records = IntStream.range(0, 5).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(toList());

        records.stream().map(TestHelpers::getListBytes).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getResult().getRecords();
        // We should have 5 records
        Assert.assertEquals(aggregate.size(), 5);
        // We should have all the records
        Assert.assertEquals(aggregate, records);

        // This is the same as the results
        Assert.assertEquals(raw.getRecords(), aggregate);
        Assert.assertEquals(raw.getMetadata().asMap(), raw.getResult().getMeta().asMap());
    }

    @Test
    public void testLimitExact() {
        Raw raw = makeRaw(10);

        List<BulletRecord> records = IntStream.range(0, 10).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(toList());

        records.stream().map(TestHelpers::getListBytes).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getResult().getRecords();
        // We should have 10 records
        Assert.assertEquals(aggregate.size(), 10);
        // We should have the all records
        Assert.assertEquals(aggregate, records);
        // This is the same as the results
        Assert.assertEquals(raw.getRecords(), aggregate);
        Assert.assertEquals(raw.getMetadata().asMap(), raw.getResult().getMeta().asMap());
    }


    @Test
    public void testLimitMoreThanMaximum() {
        Raw raw = makeRaw(10);

        List<BulletRecord> records = IntStream.range(0, 20).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(toList());

        records.stream().map(TestHelpers::getListBytes).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getResult().getRecords();
        // We should have 10 records
        Assert.assertEquals(aggregate.size(), 10);
        // We should have the first 10 records
        Assert.assertEquals(aggregate, records.subList(0, 10));
        // This is the same as the results
        Assert.assertEquals(raw.getRecords(), aggregate);
        Assert.assertEquals(raw.getMetadata().asMap(), raw.getResult().getMeta().asMap());
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

        List<BulletRecord> actual = raw.getResult().getRecords();
        Assert.assertEquals(actual.size(), 20);

        // We should have 1, 2, ... 19, 0 as the records
        List<BulletRecord> expected = IntStream.range(1, 20).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                               .collect(Collectors.toCollection(ArrayList::new));
        expected.add(RecordBox.get().add("i", 0).getRecord());
        Assert.assertEquals(actual, expected);

        // This is the same as the results
        Assert.assertEquals(raw.getRecords(), expected);
        Assert.assertEquals(raw.getMetadata().asMap(), raw.getResult().getMeta().asMap());
    }

    @Test
    public void testLimitConfiguredMaximums() {
        Raw raw = makeRaw(50000, 200);
        List<BulletRecord> records = IntStream.range(0, 300).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(toList());
        records.stream().map(TestHelpers::getListBytes).forEach(raw::combine);

        List<BulletRecord> aggregate = raw.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 200);
        Assert.assertEquals(aggregate, records.subList(0, 200));

        Assert.assertEquals(raw.getRecords(), aggregate);
        Assert.assertEquals(raw.getMetadata().asMap(), raw.getResult().getMeta().asMap());
    }

    @Test
    public void testResetting() {
        Raw raw = makeRaw(10);
        List<BulletRecord> records = IntStream.range(0, 10).mapToObj(x -> RecordBox.get().add("i", x).getRecord())
                                              .collect(toList());

        byte[] batchOfTwo = getListBytes(records.subList(0, 2).toArray(new BulletRecord[2]));
        byte[] batchOfThree = getListBytes(records.subList(2, 5).toArray(new BulletRecord[3]));
        byte[] batchOfOne = getListBytes(records.subList(5, 6).toArray(new BulletRecord[1]));
        byte[] batchOfFour = getListBytes(records.subList(6, 10).toArray(new BulletRecord[4]));

        raw.combine(batchOfTwo);
        List<BulletRecord> aggregate = raw.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 2);
        Assert.assertEquals(aggregate, records.subList(0, 2));
        Assert.assertEquals(raw.getRecords(), aggregate);
        Assert.assertEquals(raw.getMetadata().asMap(), raw.getResult().getMeta().asMap());

        raw.combine(batchOfThree);
        aggregate = raw.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 5);
        Assert.assertEquals(aggregate, records.subList(0, 5));
        Assert.assertEquals(raw.getRecords(), aggregate);
        Assert.assertEquals(raw.getMetadata().asMap(), raw.getResult().getMeta().asMap());

        raw.reset();
        raw.combine(batchOfOne);
        raw.combine(batchOfFour);
        aggregate = raw.getResult().getRecords();
        Assert.assertEquals(aggregate.size(), 5);
        Assert.assertEquals(aggregate, records.subList(5, 10));
        Assert.assertEquals(raw.getRecords(), aggregate);
        Assert.assertEquals(raw.getMetadata().asMap(), raw.getResult().getMeta().asMap());
    }
}
