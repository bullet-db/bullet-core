/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.expressionparser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class CaseInsensitiveStreamTest {
    @Test
    public void testGetSourceName() {
        String expression = "1 + 5";
        CaseInsensitiveStream stream = new CaseInsensitiveStream(new ANTLRInputStream(expression));
        assertEquals(stream.getSourceName(), "<unknown>");
    }
}
