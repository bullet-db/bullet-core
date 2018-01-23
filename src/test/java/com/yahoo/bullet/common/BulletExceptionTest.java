/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.common.BulletException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.yahoo.bullet.parsing.ParsingError.makeError;
import static java.util.Arrays.asList;

public class BulletExceptionTest {
    @Test
    public void testWrappingExceptions() {
        BulletException pe = new BulletException(asList(makeError(new NullPointerException()),
                                                        makeError("foo", "bar")));
        Assert.assertEquals(pe.getErrors().size(), 2);
    }
}
