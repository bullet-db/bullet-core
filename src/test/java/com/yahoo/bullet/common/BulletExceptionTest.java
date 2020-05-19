/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BulletExceptionTest {
    @Test
    public void testWrappingExceptions() {
        BulletException exception1 = new BulletException(new BulletError("foo", "bar"));
        BulletException exception2 = new BulletException("foo", "bar");

        Assert.assertNotNull(exception1.getError());
        Assert.assertNotNull(exception2.getError());
    }
}
