/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.aggregations;

import com.yahoo.bullet.operations.aggregations.sketches.KMVSketch;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.sketches.ResizeFactor;

/**
 * The parent class for {@link SketchingStrategy} that use the KMV type of Sketch - Theta and Tuple.
 */
public abstract class KMVStrategy<S extends KMVSketch> extends SketchingStrategy<S> {
    /**
     * Constructor that requires an {@link Aggregation}.
     *
     * @param aggregation An {@link Aggregation} with valid fields and attributes for this aggregation type.
     */
    @SuppressWarnings("unchecked")
    public KMVStrategy(Aggregation aggregation) {
        super(aggregation);
    }

    /**
     * Converts a integer representing the resizing for Sketches into a {@link ResizeFactor}.
     *
     * @param key The key to get the configured resize factor from the configuration.
     * @return A {@link ResizeFactor} represented by the integer or {@link ResizeFactor#X8} otherwise.
     */
    @SuppressWarnings("unchecked")
    public ResizeFactor getResizeFactor(String key) {
        return getResizeFactor(config.getAs(key, Integer.class));
    }
    /**
     * Converts a integer representing the resizing for Sketches into a {@link ResizeFactor}.
     *
     * @param factor An int representing the scaling when the Sketch reaches its threshold. Supports 1, 2, 4 and 8.
     * @return A {@link ResizeFactor} represented by the integer or {@link ResizeFactor#X8} otherwise.
     */
    public static ResizeFactor getResizeFactor(int factor) {
        switch (factor) {
            case 1:
                return ResizeFactor.X1;
            case 2:
                return ResizeFactor.X2;
            case 4:
                return ResizeFactor.X4;
            default:
                return ResizeFactor.X8;
        }
    }
}
