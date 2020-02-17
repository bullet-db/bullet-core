/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing;

import java.util.HashMap;
import java.util.Map;

public class WindowUtils {
    public static Window makeTumblingWindow(Integer emitValue) {
        return makeWindow(Window.Unit.TIME, emitValue);
    }

    public static Window makeSlidingWindow(Integer emitValue) {
        return makeWindow(Window.Unit.RECORD, emitValue);
    }

    public static Window makeWindow(Window.Unit emitUnit, Integer emitValue, Window.Unit includeUnit, Integer includeValue) {
        Window window = makeWindow(emitUnit, emitValue);
        if (includeUnit != null) {
            window.setInclude(makeInclude(includeUnit, includeValue));
        }
        return window;
    }

    public static Window makeWindow(Window.Unit emitUnit, Integer emitValue) {
        Window window = new Window();
        if (emitUnit != null) {
            window.setEmit(makeEmit(emitUnit, emitValue));
        }
        return window;
    }

    public static Map<String, Object> makeEmit(Window.Unit unit, Integer value) {
        return makeWindowMap(unit, Window.EMIT_EVERY_FIELD, value);
    }

    public static Map<String, Object> makeInclude(Window.Unit unit, Integer value) {
        return makeWindowMap(unit, Window.INCLUDE_FIRST_FIELD, value);
    }

    public static Map<String, Object> makeWindowMap(Window.Unit unit, String key, Integer value) {
        Map<String, Object> map = new HashMap<>();
        map.put(Window.TYPE_FIELD, unit.getName());
        if (unit != Window.Unit.ALL) {
            map.put(key, value);
        }
        return map;
    }
}
