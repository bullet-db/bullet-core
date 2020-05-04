/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.result.Clip;

public interface PostStrategy {
    /**
     * Executes the post aggregation.
     *
     * @param clip The input {@link Clip}.
     * @return The output {@link Clip}.
     */
    Clip execute(Clip clip);
}
