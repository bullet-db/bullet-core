/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.result.JSONFormatter;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

@Data
public class BulletError implements JSONFormatter, Serializable {
    private static final long serialVersionUID = -8557063189698127685L;

    public static final String ERROR_KEY = "error";
    public static final String RESOLUTIONS_KEY = "resolutions";

    private final String error;
    private final List<String> resolutions;

    @Override
    public String asJSON() {
        Map<String, Object> map = new HashMap<>();
        map.put(ERROR_KEY, error);
        map.put(RESOLUTIONS_KEY, resolutions);
        return JSONFormatter.asJSON(map);
    }

    /**
     * Creates a BulletError with the given error message and the resolution.
     *
     * @param error The message denoting the error.
     * @param resolution A resolution message.
     *
     * @return A BulletError representing this cause.
     */
    public static BulletError makeError(String error, String resolution) {
        return new BulletError(error, singletonList(resolution));
    }
}
