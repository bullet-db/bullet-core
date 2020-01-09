/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.result.Clip;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface PostStrategy extends Initializable {
    /**
     * Executes the post aggregation.
     *
     * @param clip The input {@link Clip}.
     * @return The output {@link Clip}.
     */
    Clip execute(Clip clip);

    /**
     * Default implementation for {@link Initializable}.
     *
     * @return An empty {@link Optional}.
     */
    default Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }
}
