/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.aggregations.sketches;

/**
 * This class wraps sketches that need a separate sketch for update and union operations. It manages the metadata
 * relating to managing the common operations relating to managing dual sketches. All sketches inheriting this must
 * call {@link #update()}, {@link #union()} and {@link #reset} when performing the respective operations. It must also
 * call {@link #merge()} before performing any operations that change data in the sketch such as getting metadata or
 * results or resetting.
 */
public abstract class DualSketch extends Sketch {
    private boolean updated = false;
    private boolean unioned = false;
    private boolean mustMerge = true;

    /**
     * This method must be called after an update operation.
     */
    protected void update() {
        updated = true;
        mustMerge = true;
    }

    /**
     * This method must be called after a {@link #union(byte[])}.
     */
    protected void union() {
        unioned = true;
        mustMerge = true;
    }

    /**
     * This method must be called before getting any kind of data out of the sketch. This calls the appropriate driver
     * methods to tell a subclass whether it needs to merge its two sketches ({@link #mergeBothSketches()},
     * or merge its union sketch ({@link #mergeUnionSketch()} or just the update sketch ({@link #mergeUpdateSketch()}.
     * It will also ask to add any existing merged results back into the union sketch ({@link #unionedExistingResults()}). _
     *
     * This method is idempotent and will not recollect till a data changing operation has been performed, such as an
     * update, union or a reset. This is why you must call the appropriate methods ({@link #update()}, {@link #union()},
     * {@link #reset()}) defined in this class when performing those operations.
     */
    protected void merge() {
        if (!mustMerge) {
            return;
        }
        if (unionedExistingResults()) {
            // Force unioned to be true so that the union sketch with the result is merged
            unioned = true;
        }
        if (unioned && updated) {
            mergeBothSketches();
        } else if (unioned) {
            mergeUnionSketch();
        } else {
            mergeUpdateSketch();
        }
        mustMerge = false;
    }

    /**
     * Merge and reset both the update sketch and the union sketch into the result.
     */
    protected abstract void mergeBothSketches();

    /**
     * Merge and reset just the update sketch into the result.
     */
    protected abstract void mergeUpdateSketch();

    /**
     * Merge and reset just the union sketch into the result.
     */
    protected abstract void mergeUnionSketch();

    /**
     * Merge the existing result into the union sketch if needed and return a boolean if the union was done.
     *
     * @return A boolean denoting whether the union was done.
     */
    protected abstract boolean unionedExistingResults();

    /**
     * {@inheritDoc}
     *
     * Call this after your reset operations to reset the metadata relating to storing two sketches. You should
     * reset or remove all your sketches here.
     */
    @Override
    public void reset() {
        updated = false;
        unioned = false;
        // If reset, must merge again since old merged result is thrown away and recreated.
        mustMerge = true;
    }
}
