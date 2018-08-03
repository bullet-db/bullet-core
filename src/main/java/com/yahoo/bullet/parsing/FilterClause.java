/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
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
public class FilterClause extends Clause {
    @Expose
    private String field;
    @Expose
    private List<String> values;
    @Expose
    @SerializedName("compare-to-fields")
    private Boolean compareToFields;

    // An optimization to cache the compiled patterns per FilterClause rather than redoing it per record
    private List<Pattern> patterns;

    /**
     * Default Constructor. GSON recommended.
     */
    public FilterClause() {
        field = null;
        values = null;
        operation = null;
    }

    @Override
    public void configure(BulletConfig configuration) {
        if (operation == REGEX_LIKE) {
            patterns = values.stream().map(FilterClause::compile).filter(Objects::nonNull).collect(Collectors.toList());
        }
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", " + "field: " + field + ", " + "values: " + values + "}";
    }

    private static Pattern compile(String value) {
        try {
            return Pattern.compile(value);
        } catch (PatternSyntaxException pse) {
            return null;
        }
    }
}
