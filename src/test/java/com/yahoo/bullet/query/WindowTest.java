/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WindowTest {
    @Test
    public void testDefaults() {
        Window window = new Window();
        Assert.assertNull(window.getEmitEvery());
        Assert.assertNull(window.getEmitType());
        Assert.assertNull(window.getIncludeType());
        Assert.assertNull(window.getIncludeFirst());
        Assert.assertNull(window.getType());
    }

    @Test
    public void testConfiguration() {
        BulletConfig config = new BulletConfig();

        Window window = new Window();
        window.configure(config);
        Assert.assertNull(window.getEmitType());
        Assert.assertNull(window.getIncludeType());

        window = WindowUtils.makeSlidingWindow(10);
        window.configure(config);
        Assert.assertEquals(window.getEmitType(), Window.Unit.RECORD);
        Assert.assertNull(window.getIncludeType());

        window = WindowUtils.makeSlidingWindow(10);
        window.configure(config);

        window = WindowUtils.makeTumblingWindow(1000);
        window.configure(config);
        Assert.assertEquals(window.getEmitType(), Window.Unit.TIME);
        Assert.assertEquals(window.getEmitEvery(), (Integer) 1000);

        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, 5000);
        config.validate();

        window = WindowUtils.makeTumblingWindow(1000);
        Assert.assertEquals(window.getEmitEvery(), (Integer) 1000);
        window.configure(config);
        Assert.assertEquals(window.getEmitType(), Window.Unit.TIME);
        Assert.assertEquals(window.getEmitEvery(), (Integer) 5000);
    }

    @Test
    public void testClassification() {
        Window window = new Window();
        Assert.assertNull(window.getType());
        Assert.assertFalse(window.isTimeBased());

        window = new Window(500, Window.Unit.TIME);
        Assert.assertEquals(window.getType(), Window.Classification.TIME_TIME);
        Assert.assertTrue(window.isTimeBased());

        window = new Window(500, Window.Unit.TIME);
        window.setIncludeType(Window.Unit.RECORD);
        Assert.assertEquals(window.getType(), Window.Classification.TIME_RECORD);
        Assert.assertTrue(window.isTimeBased());

        window = new Window(500, Window.Unit.TIME, Window.Unit.TIME, 500);
        Assert.assertEquals(window.getType(), Window.Classification.TIME_TIME);
        Assert.assertTrue(window.isTimeBased());

        window = new Window(500, Window.Unit.TIME, Window.Unit.ALL, null);
        Assert.assertEquals(window.getType(), Window.Classification.TIME_ALL);
        Assert.assertTrue(window.isTimeBased());

        window = new Window(500, Window.Unit.RECORD);
        Assert.assertEquals(window.getType(), Window.Classification.RECORD_RECORD);
        Assert.assertFalse(window.isTimeBased());

        window = new Window(500, Window.Unit.RECORD, Window.Unit.RECORD, 500);
        Assert.assertEquals(window.getType(), Window.Classification.RECORD_RECORD);
        Assert.assertFalse(window.isTimeBased());

        window = new Window(500, Window.Unit.RECORD);
        window.setIncludeType(Window.Unit.TIME);
        Assert.assertEquals(window.getType(), Window.Classification.RECORD_TIME);
        Assert.assertFalse(window.isTimeBased());

        window = new Window(500, Window.Unit.RECORD);
        window.setIncludeType(Window.Unit.ALL);
        Assert.assertEquals(window.getType(), Window.Classification.RECORD_ALL);
        Assert.assertFalse(window.isTimeBased());
    }

    @Test
    public void testProperInitialization() {
        BulletConfig config = new BulletConfig();
        Window window;

        window = WindowUtils.makeTumblingWindow(2000);
        window.configure(config);

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.TIME, 1000);
        window.configure(config);

        window = WindowUtils.makeSlidingWindow(1);
        window.configure(config);

        window = WindowUtils.makeWindow(Window.Unit.RECORD, 1, Window.Unit.RECORD, 1);
        window.configure(config);

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.ALL, null);
        window.configure(config);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The emit every field must be positive\\.")
    public void testImproperEmitEvery() {
        new Window(0, Window.Unit.TIME);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The emit type cannot be ALL\\.")
    public void testImproperEmit() {
        new Window(5000, Window.Unit.ALL);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The include field must match the emit field if not type ALL\\.")
    public void testImproperIncludeTypeMismatch() {
        new Window(5000, Window.Unit.TIME, Window.Unit.RECORD, 5000);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The include field must match the emit field if not type ALL\\.")
    public void testImproperIncludeTimeMismatch() {
        new Window(5000, Window.Unit.TIME, Window.Unit.TIME, 1000);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The emit type was RECORD and the include type was ALL\\.")
    public void testNoRecordAll() {
        new Window(5000, Window.Unit.RECORD, Window.Unit.ALL, null);
    }

    @Test
    public void testToString() {
        Window window = new Window(500, Window.Unit.TIME, Window.Unit.TIME, 500);
        Assert.assertEquals(window.toString(), "{emitEvery: 500, emitType: TIME, includeType: TIME, includeFirst: 500}");
    }
}
