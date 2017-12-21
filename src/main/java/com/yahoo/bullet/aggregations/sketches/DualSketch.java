/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

/**
 * This class wraps sketches that need a separate sketch for update and union operations. It manages the metadata
 * relating to managing the common operations relating to managing dual sketches. All sketches inheriting this must
 * call {@link #update()} and {@link #union()} when performing the respective operations. It must also call
 * {@link #collect()} before performing any operations that change data in the sketch such as getting metadata or
 * results or resetting.
 */
public abstract class DualSketch extends Sketch {
    private boolean updated = false;
    private boolean unioned = false;
    private boolean collected = false;

    /**
     * This method must be called after an update operation.
     */
    protected void update() {
        updated = true;
        collected = false;
    }

    /**
     * This method must be called after a {@link #union(byte[])}.
     */
    protected void union() {
        unioned = true;
        collected = false;
    }

    /**
     * This method must be called before getting any kind of data out of the sketch. This calls the appropriate driver
     * methods to tell a subclass whether it needs to collect its two sketches ({@link #collectUpdateAndUnionSketch()},
     * or collect its union sketch ({@link #collectUnionSketch()} or just the update sketch ({@link #collectUpdateSketch()}.
     *
     * This method is idempotent and will not recollect till a data changing operation has been performed, such as an
     * update, union or a reset. This is why you must call the appropriate methods ({@link #update()}, {@link #union()},
     * {@link #reset()}) defined in this class when performing those operations.
     */
    protected void collect() {
        if (collected) {
            return;
        }
        if (unioned && updated) {
            collectUpdateAndUnionSketch();
        } else if (unioned) {
            collectUnionSketch();
        } else {
            collectUpdateSketch();
        }
        collected = true;
    }

    /**
     * Collect the update sketch and the union sketch into one.
     */
    protected abstract void collectUpdateAndUnionSketch();

    /**
     * Collect the update sketch for data reading.
     */
    protected abstract void collectUpdateSketch();

    /**
     * Collect the union sketch for data reading.
     */
    protected abstract void collectUnionSketch();

    /**
     * {@inheritDoc}
     *
     * Call this after your reset operations to reset the metadata relating to storing two sketches.
     */
    @Override
    public void reset() {
        updated = false;
        unioned = false;
        collected = false;
    }
}
