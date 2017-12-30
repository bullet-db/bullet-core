/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.parsing.Error;
import lombok.Getter;

import java.util.List;

@Getter
public class BulletException extends Exception {
    private List<Error> errors;

    /**
     * Creates a BulletException from a {@link List} of {@link Error} objects.
     *
     * @param errors The errors that this should wrap.
     */
    public BulletException(List<Error> errors) {
        this.errors = errors;
    }
}
