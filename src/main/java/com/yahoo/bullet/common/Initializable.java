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

    /**
     * Tries to initialize an instance of {@link Initializable} and wraps the errors in a {@link BulletException} if
     * there were any.
     *
     * @param initializable A non-null instance to try and initialize.
     * @throws BulletException The errors wrapped as an exception if it cannot be initialized.
     */
    static void tryInitializing(Initializable initializable) throws BulletException {
        Optional<List<BulletError>> errors = initializable.initialize();
        if (errors.isPresent()) {
            throw new BulletException(errors.get());
        }
    }
}
