/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.common;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.common.BulletError.makeError;

public class BulletExceptionTest {
    @Test
    public void testWrappingExceptions() {
        BulletException pe = new BulletException(Collections.singletonList(makeError("foo", "bar")));
        Assert.assertEquals(pe.getErrors().size(), 1);
    }
}
