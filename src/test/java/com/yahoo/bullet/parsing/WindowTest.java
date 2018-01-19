/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import org.testng.Assert;
import org.testng.annotations.Test;

public class WindowTest {
    @Test
    public void testDefaults() {
        Window window = new Window();
        Assert.assertNull(window.getEmit());
        Assert.assertNull(window.getInclude());
        Assert.assertNull(window.getType());
        Assert.assertNull(window.getEmitType());
        Assert.assertNull(window.getIncludeType());
    }
}
