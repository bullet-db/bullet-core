/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import org.testng.Assert;
import org.testng.annotations.Test;

import static com.yahoo.bullet.parsing.Error.makeError;
import static java.util.Arrays.asList;

public class ParsingExceptionTest {
    @Test
    public void testParsingException() {
        ParsingException pe = new ParsingException(asList(makeError(new NullPointerException()),
                                                          makeError(new ArrayIndexOutOfBoundsException())));
        Assert.assertEquals(pe.getErrors().size(), 2);
    }
}
