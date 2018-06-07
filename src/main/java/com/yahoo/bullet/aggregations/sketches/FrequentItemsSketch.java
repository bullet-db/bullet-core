/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.record.BulletAvroRecord;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta.Concept;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfUtf16StringsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.result.Meta.addIfNonNull;

/**
 * Wraps a {@link ItemsSketch} of String.
 */
public class FrequentItemsSketch extends Sketch {
    private ItemsSketch<String> sketch;

    private final ErrorType type;
    private final long threshold;
    private final int maxSize;

    // No state -> static
    private static final ArrayOfItemsSerDe<String> SER_DE = new ArrayOfUtf16StringsSerDe();

    public static final String ITEM_FIELD = "item";
    public static final String COUNT_FIELD = "count";

    /**
     * Creates a FrequentItemsSketch with the given {@link ErrorType}, the maximum map entries, and threshold.
     *
     * @param type The {@link ErrorType} for the Sketch.
     * @param maxMapCapacity The maximum power of 2 entries for the Sketch used as the internal map size.
     * @param threshold The threshold that will be used for selecting items if the Sketch error is less than it.
     * @param maxSize The maximum size of the number of frequent items.
     */
    public FrequentItemsSketch(ErrorType type, int maxMapCapacity, long threshold, int maxSize) {
        this.type = type;
        this.threshold = threshold;
        this.maxSize = maxSize;
        sketch = new ItemsSketch<>(maxMapCapacity);
    }

    /**
     * Creates a FrequentItemsSketch with the given {@link ErrorType} and the maximum map entries.
     *
     * @param type The {@link ErrorType} for the Sketch.
     * @param maxMapCapacity The maximum power of 2 entries for the Sketch used as the internal map size.
     * @param maxSize The maximum size of the number of frequent items.
     */
    public FrequentItemsSketch(ErrorType type, int maxMapCapacity, int maxSize) {
        // Using -1 guarantees that the Sketch will use its error rather than the -1 threshold.
        this(type, maxMapCapacity, -1L, maxSize);
    }

    /**
     * Inserts an item into the Sketch.
     *
     * @param item The String item to add to the Sketch.
     */
    public void update(String item) {
        sketch.update(item);
    }

    @Override
    public void union(byte[] serialized) {
        ItemsSketch<String> other = ItemsSketch.getInstance(new NativeMemory(serialized), SER_DE);
        sketch.merge(other);
    }

    @Override
    public List<BulletRecord> getRecords() {
        List<BulletRecord> data = new ArrayList<>();

        ItemsSketch.Row<String>[] items = sketch.getFrequentItems(threshold, type);
        for (int i = 0; i < items.length && i < maxSize; ++i) {
            ItemsSketch.Row<String> item = items[i];
            BulletRecord record = new BulletAvroRecord();
            record.setString(ITEM_FIELD, item.getItem());
            record.setLong(COUNT_FIELD, item.getEstimate());
            data.add(record);
        }
        return data;
    }

    @Override
    public byte[] serialize() {
        return sketch.toByteArray(SER_DE);
    }

    @Override
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        Clip data = super.getResult(metaKey, conceptKeys);
        data.add(getRecords());
        return data;
    }

    @Override
    protected Map<String, Object> addMetadata(Map<String, String> conceptKeys) {
        Map<String, Object> metadata = super.addMetadata(conceptKeys);
        addIfNonNull(metadata, conceptKeys, Concept.SKETCH_ITEMS_SEEN, this::getStreamLength);
        addIfNonNull(metadata, conceptKeys, Concept.SKETCH_ACTIVE_ITEMS, this::getItemsStored);
        addIfNonNull(metadata, conceptKeys, Concept.SKETCH_MAXIMUM_COUNT_ERROR, this::getMaximumError);
        return metadata;
    }

    @Override
    public void reset() {
        sketch.reset();
    }

    @Override
    protected String getFamily() {
        return Family.FREQUENCY.getFamilyName();
    }

    @Override
    protected Boolean isEstimationMode() {
        return sketch.getMaximumError() > 0;
    }

    @Override
    protected Integer getSize() {
        // Size is dependent on the items, so not computing it. Could use length of serialize
        return null;
    }

    private Long getStreamLength() {
        return sketch.getStreamLength();
    }

    private Integer getItemsStored() {
        return sketch.getNumActiveItems();
    }

    private Long getMaximumError() {
        return sketch.getMaximumError();
    }
}
