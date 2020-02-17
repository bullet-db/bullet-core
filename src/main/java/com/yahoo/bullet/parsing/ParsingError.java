/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletError;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;

import static java.util.Collections.singletonList;

public class ParsingError extends BulletError {
    private static final long serialVersionUID = 4559080045003878180L;

    public static final String GENERIC_JSON_ERROR = "Unable to parse the given JSON";
    public static final String GENERIC_JSON_RESOLUTION = "Please recheck your JSON with a linter or compare " +
                                                         "it against the Bullet query specification";

    /**
     * Constructor override.
     *
     * @param error The String error that represents the error.
     * @param resolutions The {@link List} of String messages that represent the resolutions.
     */
    public ParsingError(String error, List<String> resolutions) {
        super(error, resolutions);
    }

    /**
     * Creates a ParsingError object with the root cause message from the given cause if available.
     *
     * @param cause A cause if available.
     *
     * @return A ParsingError representing this cause.
     */
    public static ParsingError makeError(Throwable cause) {
        String message = ExceptionUtils.getRootCauseMessage(cause);
        message = message.isEmpty() ? GENERIC_JSON_ERROR : message;
        return new ParsingError(message, singletonList(GENERIC_JSON_RESOLUTION));
    }

    /**
     * Creates a ParsingError object with the original query string and the root cause message from the given cause.
     *
     * @param cause A cause.
     * @param queryString The original query.
     *
     * @return A ParsingError representing this cause.
     */
    public static ParsingError makeError(RuntimeException cause, String queryString) {
        String message = ExceptionUtils.getRootCauseMessage(cause);
        message = message.isEmpty() ? "" : message;
        return new ParsingError(GENERIC_JSON_ERROR + ":\n" + queryString + "\n" + message,
                                singletonList(GENERIC_JSON_RESOLUTION));
    }
}
