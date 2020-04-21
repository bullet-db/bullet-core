/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter @AllArgsConstructor
public class BulletException extends RuntimeException {
    private static final long serialVersionUID = 2868933191828758133L;

    private BulletError error;

    /**
     * Creates a BulletException from an error and resolution.
     *
     * @param error The error message.
     * @param resolution The resolution message.
     */
    public BulletException(String error, String resolution) {
        this.error = new BulletError(error, resolution);
    }
}
