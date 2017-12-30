/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import com.yahoo.bullet.result.JSONFormatter;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class BulletError implements JSONFormatter {
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
}
