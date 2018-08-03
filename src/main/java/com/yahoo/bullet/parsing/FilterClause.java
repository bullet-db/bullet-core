/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.common.BulletConfig;
import lombok.AllArgsConstructor;
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
    @Getter @AllArgsConstructor
    public static class Value {
        public enum Type {
            @SerializedName("VALUE")
            VALUE,
            @SerializedName("FIELD")
            FIELD,
            @SerializedName("CAST")
            CAST
        }
        @Expose
        Type type;
        @Expose
        String value;

        @Override
        public String toString() {
            return "{type: " + type + ", " + "value: " + value + "}";
        }
    }
    @Expose
    private String field;
    @Expose
    private List<Object> values;

    private static final Gson GSON = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

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
        values = values.stream().map(v -> v instanceof String ? new Value(Value.Type.VALUE, (String) v) : GSON.fromJson(GSON.toJson(v), Value.class)).collect(Collectors.toList());
        if (operation == REGEX_LIKE) {
            patterns = values.stream().map(FilterClause::compile).filter(Objects::nonNull).collect(Collectors.toList());
        }
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", " + "field: " + field + ", " + "values: " + values + "}";
    }

    private static Pattern compile(Object value) {
        try {
            return Pattern.compile(((Value) value).getValue());
        } catch (PatternSyntaxException pse) {
            return null;
        }
    }
}
