/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.common.Utilities;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;
import static java.util.Collections.singletonList;

@Getter @Setter @Slf4j
public class Window implements Configurable, Initializable {

    public static final int SINGLE_RECORD = 1;

    /**
     * Represents the type of the Window Unit for either emit or include.
     */
    @Getter
    public enum Unit {
        RECORD("RECORD"), TIME("TIME"), ALL("ALL");

        private String name;

        Unit(String name) {
            this.name = name;
        }

        /**
         * Checks to see if this String represents this enum.
         *
         * @param name The String version of the enum.
         * @return true if the name represents this enum.
         */
        public boolean isMe(String name) {
            return this.name.equalsIgnoreCase(name);
        }
    }
    public static final Map<String, Unit> SUPPORTED_TYPES = new HashMap<>();
    static {
        SUPPORTED_TYPES.put(Unit.TIME.getName(), Unit.TIME);
        SUPPORTED_TYPES.put(Unit.RECORD.getName(), Unit.RECORD);
        SUPPORTED_TYPES.put(Unit.ALL.getName(), Unit.ALL);
    }

    /**
     * Represents the type of window.
     */
    public enum Classification {
        TIME_TIME, RECORD_RECORD, TIME_RECORD, RECORD_TIME, TIME_ALL, RECORD_ALL
    }

    public static final String TYPE_FIELD = "type";
    public static final String EMIT_EVERY_FIELD = "every";
    public static final String INCLUDE_LAST_FIELD = "last";

    public static final BulletError IMPROPER_EMIT = makeError("The \"type\" field was not found or had bad values",
                                                              "Please set \"type\" to one of: \"TIME\" or \"RECORD\"");
    public static final BulletError ONLY_ONE_RECORD = makeError("The \"every\" field had bad values",
                                                                "Please set \"every\" to 1");
    public static final BulletError IMPROPER_INCLUDE = makeError("The \"type\" field had bad values.",
                                                                 "Please set \"type\" to one of: \"ALL\", \"RECORD\"");
    public static final BulletError UNSUPPORTED_LAST = makeError("The \"last\" field was set",
                                                                 "It is unsupported. It should be removed.");

    @Expose
    private Map<String, Object> emit;
    @Expose
    private Map<String, Object> include;

    private Unit emitType;
    private Unit includeType;

    /**
     * Default constructor. GSON recommended.
     */
    public Window() {
        emit = null;
        include = null;
    }

    @Override
    public void configure(BulletConfig config) {
        emitType = getUnit(emit);
        includeType = getUnit(include);
        if (Utilities.isEmpty(emit) || emitType != Unit.TIME) {
            return;
        }
        Number every = Utilities.getCasted(emit, EMIT_EVERY_FIELD, Number.class);
        if (every != null) {
            int minEmitTime = config.getAs(BulletConfig.WINDOW_MIN_EMIT_EVERY, Integer.class);
            // Clamp upward to minimum
            if (every.intValue() < minEmitTime) {
                emit.put(EMIT_EVERY_FIELD, minEmitTime);
            }
        }
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (emitType == Unit.ALL) {
            return Optional.of(singletonList(IMPROPER_EMIT));
        }

        Number every = Utilities.getCasted(emit, EMIT_EVERY_FIELD, Number.class);
        if (emitType == Unit.RECORD && every.intValue() != SINGLE_RECORD) {
            return Optional.of(singletonList(ONLY_ONE_RECORD));
        }

        if (includeType == Unit.TIME) {
            return Optional.of(singletonList(IMPROPER_INCLUDE));
        }
        return Optional.empty();
    }

    private static Unit getUnit(Map<String, Object> map) {
        if (Utilities.isEmpty(map)) {
            return null;
        }
        String type = Utilities.getCasted(map, TYPE_FIELD, String.class);
        return SUPPORTED_TYPES.get(type);
    }

    /**
     * Gets the classification of this window for the given emit and include types.
     *
     * @return The {@link Classification} of this window.
     */
    public Classification getType() {
        return classify(emitType, includeType);
    }

    /**
     * Returns true if this is a time based window (emits based on time).
     *
     * @return A boolean denoting whether this window is a time based window.
     */
    public boolean isTimeBased() {
        return emitType == Unit.TIME;
    }

    /**
     * Get the classification of the window for the given emit and include types. Exposed for testing.
     *
     * @param emitType The type of the emit.
     * @param includeType The type of the include.
     * @return A {@link Classification} for the window.
     */
    static Classification classify(Unit emitType, Unit includeType) {
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

    @Override
    public String toString() {
        return "{emit: " + emit + ", include: " + include + "}";
    }
}
