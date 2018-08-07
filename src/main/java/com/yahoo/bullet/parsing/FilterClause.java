/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static com.yahoo.bullet.parsing.Clause.Operation.REGEX_LIKE;

@Slf4j @Getter @Setter
public abstract class FilterClause<T> extends Clause {
    @Expose
    protected String field;

    @Expose
    protected List<T> values;

    // An optimization to cache the compiled patterns per FilterClause rather than redoing it per record
    protected List<Pattern> patterns;

    public static final String VALUES_FIELD = "values";

    /**
     * Default Constructor. GSON recommended.
     */
    public FilterClause() {
        field = null;
        operation = null;
        values = null;
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", " + "field: " + field + ", " + "values: " + values + "}";
    }

    /**
     * Get the value string from an object.
     *
     * @param value The value object to get from.
     * @return The value string.
     */
    public abstract String getValue(T value);

    @Override
    public void configure(BulletConfig configuration) {
        if (operation == REGEX_LIKE) {
            patterns = values.stream().map(v -> FilterClause.compile(getValue(v))).filter(Objects::nonNull).collect(Collectors.toList());
        }
    }

    private static Pattern compile(String value) {
        try {
            return Pattern.compile(value);
        } catch (PatternSyntaxException pse) {
            return null;
        }
    }
}
