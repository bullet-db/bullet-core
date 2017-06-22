/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import java.util.List;
import java.util.Optional;

public interface Validatable {
    /**
     * Validates this object.
     *
     * @return An optional list of errors in this object or its constituents.
     */
    Optional<List<Error>> validate();
}
