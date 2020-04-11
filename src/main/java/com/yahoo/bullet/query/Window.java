/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.windowing.AdditiveTumbling;
import com.yahoo.bullet.windowing.Basic;
import com.yahoo.bullet.windowing.Scheme;
import com.yahoo.bullet.windowing.SlidingRecord;
import com.yahoo.bullet.windowing.Tumbling;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Objects;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter @NoArgsConstructor @Slf4j
public class Window implements Configurable, Serializable {
    private static final long serialVersionUID = 3671691728693727956L;

    /** Represents the type of the Window Unit for either emit or include. */
    @Getter @AllArgsConstructor
    public enum Unit {
        RECORD("RECORD"), TIME("TIME"), ALL("ALL");

        private String name;
    }

    /**
     * Represents the type of window.
     */
    public enum Classification {
        TIME_TIME, RECORD_RECORD, TIME_RECORD, RECORD_TIME, TIME_ALL, RECORD_ALL
    }

    public static final String TYPE_FIELD = "type";
    public static final String EMIT_EVERY_FIELD = "every";
    public static final String INCLUDE_FIRST_FIELD = "first";

    public static final BulletError IMPROPER_EMIT = makeError("The emit type was missing or had bad values",
                                                              "Please set type to one of: \"TIME\" or \"RECORD\"");
    public static final BulletError IMPROPER_EVERY = makeError("The every field was missing or had bad values",
                                                              "Please set every to a positive integer");
    public static final BulletError IMPROPER_INCLUDE = makeError("The include field has to match emit or have type " +
                                                                   "\"ALL\" for emit type \"TIME\" or not be set",
                                                                 "Please remove include or match it to emit");
    public static final BulletError IMPROPER_FIRST = makeError("The first field should not be set for \"ALL\"",
                                                              "Please remove the first field");
    public static final BulletError NO_RECORD_ALL = makeError("The emit type was \"RECORD\" and include type was \"ALL\"",
                                                              "Please set emit type to \"TIME\" or match include to emit");

    private int emitEvery;
    private Unit emitType;
    private Unit includeType;
    private int includeFirst;

    public Window(int emitEvery, Unit emitType) {
        if (emitEvery <= 0) {
            throw new IllegalArgumentException("bad");
        }
        if (emitType == Unit.ALL) {
            throw new IllegalArgumentException("bad emit type");
        }
        this.emitEvery = emitEvery;
        this.emitType = Objects.requireNonNull(emitType);
    }

    public Window(int emitEvery, Unit emitType, Unit includeType, int includeFirst) {
        if (emitEvery <= 0) {
            throw new IllegalArgumentException("bad");
        }
        if (emitType == Unit.ALL) {
            throw new IllegalArgumentException("bad emit type");
        }
        // This is temporary. For now, emit needs to be equal to include. Change when other windows are supported.
        if (includeType != emitType) {
            throw new IllegalArgumentException("include type and emit type must be the same");
        }
        if (includeFirst != emitEvery) {
            throw new IllegalArgumentException("include first and emit every must be the same");
        }
        this.emitEvery = emitEvery;
        this.emitType = Objects.requireNonNull(emitType);
        this.includeType = Objects.requireNonNull(includeType);
        this.includeFirst = includeFirst;
    }

    @Override
    public void configure(BulletConfig config) {
        if (emitType != Unit.TIME) {
            return;
        }
        int minEmitTime = config.getAs(BulletConfig.WINDOW_MIN_EMIT_EVERY, Integer.class);
        // Clamp upward to minimum
        if (emitEvery < minEmitTime) {
            emitEvery = minEmitTime;
        }
    }

    public Scheme getScheme(Strategy strategy, BulletConfig config) {
        /*
         * TODO: Support other windows
         * The windows we support at the moment:
         * 1. No window -> Basic
         * 2. Window is emit RECORD and include RECORD -> SlidingRecord
         * 3. Window is emit TIME and include ALL -> Additive Tumbling
         * 4. All other windows -> Tumbling (RAW can be Tumbling too)
         */
        if (emitType == null) {
            return new Basic(strategy, null, config);
        }
        Window.Classification classification = getType();
        if (classification == Window.Classification.RECORD_RECORD) {
            return new SlidingRecord(strategy, this, config);
        }
        if (classification == Window.Classification.TIME_ALL) {
            return new AdditiveTumbling(strategy, this, config);
        }
        return new Tumbling(strategy, this, config);
    }

    /**
     * Gets the classification of this window for the given emit and include types.
     *
     * @return The {@link Classification} of this window.
     */
    public Classification getType() {
        if (emitType == Unit.TIME) {
            if (includeType == null || includeType == Unit.TIME) {
                return Classification.TIME_TIME;
            } else if (includeType == Unit.RECORD) {
                return Classification.TIME_RECORD;
            }
            return Classification.TIME_ALL;
        } else if (emitType == Unit.RECORD) {
            if (includeType == null || includeType == Unit.RECORD) {
                return Classification.RECORD_RECORD;
            } else if (includeType == Unit.TIME) {
                return Classification.RECORD_TIME;
            }
            return Classification.RECORD_ALL;
        }
        return null;
    }

    /**
     * Returns true if this is a time based window (emits based on time).
     *
     * @return A boolean denoting whether this window is a time based window.
     */
    public boolean isTimeBased() {
        return emitType == Unit.TIME;
    }

    @Override
    public String toString() {
        return "{emitEvery: " + emitEvery + ", emitType: " + emitType + ", includeType: " + includeType + ", includeFirst: " + includeFirst + "}";
    }
}
