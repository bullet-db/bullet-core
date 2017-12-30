/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import java.util.List;
import java.util.Optional;

public interface Initializable {
    /**
     * Validates and initializes this object.
     *
     * @return An {@link Optional} {@link List} of {@link BulletError} in this object or its constituents.
     */
    Optional<List<BulletError>> initialize();
}
