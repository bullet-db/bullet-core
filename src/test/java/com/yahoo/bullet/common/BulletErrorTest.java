/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BulletErrorTest {
    @Test
    public void testToString() {
        BulletError error = BulletError.makeError("foo", "bar");
        Assert.assertEquals(error.toString(), "{error: foo, resolutions: [bar]}");
    }
}
