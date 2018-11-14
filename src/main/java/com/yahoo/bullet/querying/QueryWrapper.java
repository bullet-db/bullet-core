/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.Query;

public interface QueryWrapper {
    /**
     * Get a {@link Query} instance from this wrapper object.
     *
     * @return The wrapped {@link Query}.
     */
    Query getQuery();
}
