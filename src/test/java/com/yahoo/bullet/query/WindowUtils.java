/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query;

public class WindowUtils {
    public static Window makeTumblingWindow(Integer emitValue) {
        return new Window(emitValue, Window.Unit.TIME);
    }

    public static Window makeSlidingWindow(Integer emitValue) {
        return new Window(emitValue, Window.Unit.RECORD);
    }

    public static Window makeWindow(Window.Unit emitUnit, Integer emitValue, Window.Unit includeUnit, Integer includeValue) {
        if (includeUnit != null) {
            return new Window(emitValue, emitUnit, includeUnit, includeValue);
        }
        return new Window(emitValue, emitUnit);
    }
}
