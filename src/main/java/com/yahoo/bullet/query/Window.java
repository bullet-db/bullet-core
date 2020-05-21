/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.querying.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.windowing.AdditiveTumbling;
import com.yahoo.bullet.windowing.Basic;
import com.yahoo.bullet.windowing.Scheme;
import com.yahoo.bullet.windowing.SlidingRecord;
import com.yahoo.bullet.windowing.Tumbling;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Objects;

@Getter @NoArgsConstructor @Slf4j
public class Window implements Configurable, Serializable {
    private static final long serialVersionUID = 3671691728693727956L;

    /**
     * Represents the type of the window unit for either emit or include.
     */
    public enum Unit {
        RECORD,
        TIME,
        ALL
    }

    /**
     * Represents the type of window.
     */
    public enum Classification {
        TIME_TIME,
        RECORD_RECORD,
        TIME_RECORD,
        RECORD_TIME,
        TIME_ALL,
        RECORD_ALL
    }

    public static final BulletException IMPROPER_EMIT = new BulletException("The emit type cannot be ALL.",
                                                                            "Please set type to one of: TIME or RECORD");
    public static final BulletException IMPROPER_EVERY = new BulletException("The emit every field must be positive.",
                                                                             "Please set the emit every field to a positive integer.");
    public static final BulletException IMPROPER_INCLUDE = new BulletException("The include field must match the emit field if not type ALL.",
                                                                               "Please match the include field to the emit field.");
    public static final BulletException NO_RECORD_ALL = new BulletException("The emit type was RECORD and the include type was ALL.",
                                                                            "Please set the emit type to TIME or match the include type to the emit type.");

    private Integer emitEvery;
    private Unit emitType;
    // Exposed for testing only
    @Setter(AccessLevel.PACKAGE)
    private Unit includeType;
    private Integer includeFirst;

    /**
     * Constructor that creates a window with only emit fields.
     *
     * @param emitEvery The non-null emit every field.
     * @param emitType The non-null emit unit type.
     */
    public Window(Integer emitEvery, Unit emitType) {
        this.emitEvery = Objects.requireNonNull(emitEvery);
        this.emitType = Objects.requireNonNull(emitType);
        if (emitEvery <= 0) {
            throw IMPROPER_EVERY;
        }
        if (emitType == Unit.ALL) {
            throw IMPROPER_EMIT;
        }
    }

    /**
     * Constructor that creates a window with emit and include fields.
     *
     * @param emitEvery The non-null emit every field.
     * @param emitType The non-null emit unit type.
     * @param includeType The non-null include unit type.
     * @param includeFirst The include first field. Can be null if the include unit type is ALL.
     */
    public Window(Integer emitEvery, Unit emitType, Unit includeType, Integer includeFirst) {
        this(emitEvery, emitType);
        this.includeType = Objects.requireNonNull(includeType);
        // This is temporary. For now, emit needs to be equal to include if include is not ALL.
        // Change when other windows are supported.
        switch (includeType) {
            case TIME:
            case RECORD:
                Objects.requireNonNull(includeFirst);
                if (includeType != emitType || includeFirst.intValue() != emitEvery.intValue()) {
                    throw IMPROPER_INCLUDE;
                }
                this.includeFirst = includeFirst;
                break;
            default:
                if (emitType == Unit.RECORD) {
                    throw NO_RECORD_ALL;
                }
                break;
        }
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

    /**
     * Returns the appropriate window scheme based on this window's classification.
     *
     * The windows we support at the moment:
     * 1. No window -> Basic
     * 2. Window is emit RECORD and include RECORD -> SlidingRecord
     * 3. Window is emit TIME and include ALL -> Additive Tumbling
     * 4. All other windows -> Tumbling (RAW can be Tumbling too)
     *
     * @param strategy The {@link Strategy} to be passed on to the window scheme.
     * @param config The {@link BulletConfig} to be passed on to the window scheme.
     * @return The appropriate window scheme based on this window's classification.
     */
    public Scheme getScheme(Strategy strategy, BulletConfig config) {
        // TODO: Support other windows
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
