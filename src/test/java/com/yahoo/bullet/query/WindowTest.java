/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.singletonList;

public class WindowTest {
    @Test
    public void testWindowUnitIdentifying() {
        Assert.assertTrue(Window.Unit.RECORD.isMe("record"));
        Assert.assertTrue(Window.Unit.RECORD.isMe("RECORD"));
        Assert.assertFalse(Window.Unit.RECORD.isMe(""));
        Assert.assertFalse(Window.Unit.RECORD.isMe(null));
        Assert.assertTrue(Window.Unit.ALL.isMe("all"));
        Assert.assertTrue(Window.Unit.TIME.isMe("time"));
    }

    @Test
    public void testDefaults() {
        Window window = new Window();
        Assert.assertNull(window.getEmit());
        Assert.assertNull(window.getInclude());
        Assert.assertNull(window.getType());
        Assert.assertNull(window.getEmitType());
        Assert.assertNull(window.getIncludeType());
    }

    @Test
    public void testConfiguration() {
        BulletConfig config = new BulletConfig();

        Window window = new Window();
        window.configure(config);
        Assert.assertNull(window.getEmitType());
        Assert.assertNull(window.getIncludeType());

        window = WindowUtils.makeWindow(Window.Unit.RECORD, 10);
        window.configure(config);
        Assert.assertEquals(window.getEmitType(), Window.Unit.RECORD);
        Assert.assertNull(window.getIncludeType());

        window = WindowUtils.makeWindow(Window.Unit.RECORD, 10);
        window.configure(config);

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000);
        window.configure(config);
        Assert.assertEquals(window.getEmitType(), Window.Unit.TIME);
        Assert.assertEquals(window.getEmit().get(Window.EMIT_EVERY_FIELD), 1000);

        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, 5000);
        config.validate();
        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000);
        Assert.assertEquals(window.getEmit().get(Window.EMIT_EVERY_FIELD), 1000);
        window.configure(config);
        Assert.assertEquals(window.getEmitType(), Window.Unit.TIME);
        Assert.assertEquals(window.getEmit().get(Window.EMIT_EVERY_FIELD), 5000);
    }

    @Test
    public void testClassification() {
        Window window = new Window();

        window.setEmitType(Window.Unit.TIME);
        window.setIncludeType(null);
        Assert.assertEquals(window.getType(), Window.Classification.TIME_TIME);
        Assert.assertTrue(window.isTimeBased());

        window.setEmitType(Window.Unit.TIME);
        window.setIncludeType(Window.Unit.TIME);
        Assert.assertEquals(window.getType(), Window.Classification.TIME_TIME);
        Assert.assertTrue(window.isTimeBased());

        window.setEmitType(Window.Unit.TIME);
        window.setIncludeType(Window.Unit.RECORD);
        Assert.assertEquals(window.getType(), Window.Classification.TIME_RECORD);
        Assert.assertTrue(window.isTimeBased());

        window.setEmitType(Window.Unit.TIME);
        window.setIncludeType(Window.Unit.ALL);
        Assert.assertEquals(window.getType(), Window.Classification.TIME_ALL);
        Assert.assertTrue(window.isTimeBased());

        window.setEmitType(Window.Unit.RECORD);
        window.setIncludeType(null);
        Assert.assertEquals(window.getType(), Window.Classification.RECORD_RECORD);
        Assert.assertFalse(window.isTimeBased());

        window.setEmitType(Window.Unit.RECORD);
        window.setIncludeType(Window.Unit.RECORD);
        Assert.assertEquals(window.getType(), Window.Classification.RECORD_RECORD);
        Assert.assertFalse(window.isTimeBased());

        window.setEmitType(Window.Unit.RECORD);
        window.setIncludeType(Window.Unit.TIME);
        Assert.assertEquals(window.getType(), Window.Classification.RECORD_TIME);
        Assert.assertFalse(window.isTimeBased());

        Assert.assertFalse(window.isTimeBased());
        window.setEmitType(Window.Unit.RECORD);
        window.setIncludeType(Window.Unit.ALL);
        Assert.assertEquals(window.getType(), Window.Classification.RECORD_ALL);
        Assert.assertFalse(window.isTimeBased());

        window.setEmitType(null);
        window.setIncludeType(null);
        Assert.assertNull(window.getType());
        Assert.assertFalse(window.isTimeBased());
    }

    @Test
    public void testProperInitialization() {
        BulletConfig config = new BulletConfig();
        Window window;
        Optional<List<BulletError>> errors;

        window = WindowUtils.makeTumblingWindow(2000);
        window.configure(config);
        Assert.assertFalse(window.initialize().isPresent());

        window = WindowUtils.makeWindow(Window.Unit.TIME, 2000);
        window.configure(config);
        Assert.assertFalse(window.initialize().isPresent());

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.TIME, 1000);
        window.configure(config);
        errors = window.initialize();
        Assert.assertFalse(errors.isPresent());

        window = WindowUtils.makeSlidingWindow(1);
        window.configure(config);
        Assert.assertFalse(window.initialize().isPresent());

        window = WindowUtils.makeWindow(Window.Unit.RECORD, 1, Window.Unit.RECORD, 1);
        window.configure(config);
        errors = window.initialize();
        Assert.assertFalse(errors.isPresent());

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.ALL, null);
        window.configure(config);
        errors = window.initialize();
        Assert.assertFalse(errors.isPresent());
    }

    @Test
    public void testErrorsInInitialization() {
        BulletConfig config = new BulletConfig();
        Window window;
        Optional<List<BulletError>> errors;

        window = new Window();
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_EMIT));

        window = WindowUtils.makeWindow(Window.Unit.ALL, 10);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_EMIT));

        window = WindowUtils.makeWindow(Window.Unit.TIME, null);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_EVERY));

        window = WindowUtils.makeWindow(Window.Unit.RECORD, -1);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_EVERY));

        window = WindowUtils.makeWindow(Window.Unit.RECORD, 0);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_EVERY));

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.ALL, null);
        window.getInclude().put(Window.INCLUDE_FIRST_FIELD, 1000);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_FIRST));

        window = WindowUtils.makeWindow(Window.Unit.RECORD, 1, Window.Unit.ALL, null);
        window.getInclude().put(Window.INCLUDE_FIRST_FIELD, 10);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.NO_RECORD_ALL));

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1, Window.Unit.ALL, null);
        window.getInclude().put(Window.INCLUDE_FIRST_FIELD, 10);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_FIRST));

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.TIME, null);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_INCLUDE));

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.TIME, 2000);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_INCLUDE));

        window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.RECORD, 5);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_INCLUDE));

        window = WindowUtils.makeWindow(Window.Unit.RECORD, 1, Window.Unit.RECORD, 5);
        window.configure(config);
        errors = window.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Window.IMPROPER_INCLUDE));
    }
}
