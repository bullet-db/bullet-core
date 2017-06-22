/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.result.JSONFormatter;
import lombok.Data;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

@Data(staticConstructor = "of")
public class Error implements JSONFormatter {
    public static final String GENERIC_JSON_ERROR = "Unable to parse the given JSON";
    public static final String GENERIC_JSON_RESOLUTION = "Please recheck your JSON with a linter or compare " +
                                                         "it against the Bullet query specification";

    public static final String ERROR_KEY = "error";
    public static final String RESOLUTIONS_KEY = "resolutions";

    private final String error;
    private final List<String> resolutions;

    /**
     * Creates an Error object with the root cause message from the given cause if available.
     *
     * @param cause A cause if available.
     *
     * @return An Error representing this cause.
     */
    public static Error makeError(Throwable cause) {
        String message = ExceptionUtils.getRootCauseMessage(cause);
        message = message.isEmpty() ? GENERIC_JSON_ERROR : message;
        return Error.of(message, singletonList(GENERIC_JSON_RESOLUTION));
    }

    /**
     * Creates an Error object with the original query string and the root cause message from the given cause.
     *
     * @param cause A cause.
     * @param queryString The original query.
     *
     * @return An Error representing this cause.
     */
    public static Error makeError(RuntimeException cause, String queryString) {
        String message = ExceptionUtils.getRootCauseMessage(cause);
        message = message.isEmpty() ? "" : message;
        return Error.of(GENERIC_JSON_ERROR + ":\n" + queryString + "\n" + message, singletonList(GENERIC_JSON_RESOLUTION));
    }

    /**
     * Creates an Error object with the root cause message from the given cause if available.
     *
     * @param error A description of the error.
     * @param resolution A description of a possible resolution.
     *
     * @return An Error representing this cause.
     */
    public static Error makeError(String error, String resolution) {
        return Error.of(error, singletonList(resolution));
    }

    @Override
    public String asJSON() {
        Map<String, Object> map = new HashMap<>();
        map.put(ERROR_KEY, error);
        map.put(RESOLUTIONS_KEY, resolutions);
        return JSONFormatter.asJSON(map);
    }
}
