/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import lombok.Getter;

import java.util.List;

@Getter
public class ParsingException extends Exception {
    private List<Error> errors;

    /**
     * Creates a ParsingException from a {@link List} of {@link Error} objects.
     *
     * @param errors The errors that this should wrap.
     */
    public ParsingException(List<Error> errors) {
        this.errors = errors;
    }
}
