/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.JsonParseException;
import org.testng.annotations.Test;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static com.yahoo.bullet.parsing.ParsingError.GENERIC_JSON_ERROR;
import static com.yahoo.bullet.parsing.ParsingError.GENERIC_JSON_RESOLUTION;
import static com.yahoo.bullet.parsing.ParsingError.makeError;
import static com.yahoo.bullet.result.JSONFormatter.asJSON;
import static java.util.Collections.singletonList;

public class ParsingErrorTest {
    @Test
    public void testError() {
        Throwable madeUp = new Exception("Something");

        assertJSONEquals(makeError(madeUp).asJSON(), asJSON(new ParsingError("Exception: Something", singletonList(GENERIC_JSON_RESOLUTION))));
        assertJSONEquals(makeError(null).asJSON(), asJSON(new ParsingError(GENERIC_JSON_ERROR, singletonList(GENERIC_JSON_RESOLUTION))));
    }

    @Test
    public void testJSONError() {
        JsonParseException madeUp = new JsonParseException("Something JSON", null);

        assertJSONEquals(makeError(madeUp, "JSON Query").asJSON(),
                         asJSON(new ParsingError(GENERIC_JSON_ERROR + ":\nJSON Query\nJsonParseException: Something JSON",
                                                 singletonList(GENERIC_JSON_RESOLUTION))));
        assertJSONEquals(makeError((JsonParseException) null, "JSON Query").asJSON(),
                         asJSON(new ParsingError(GENERIC_JSON_ERROR + ":\nJSON Query\n", singletonList(GENERIC_JSON_RESOLUTION))));
    }
}
