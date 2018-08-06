/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
     * Pattern compiler Method.
     *
     * @param value The String regex.
     * @return The Pattern parsed from regex.
     */
    protected static Pattern compile(String value) {
        try {
            return Pattern.compile(value);
        } catch (PatternSyntaxException pse) {
            return null;
        }
    }
}
