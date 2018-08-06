/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.common.BulletConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.yahoo.bullet.parsing.Clause.Operation.REGEX_LIKE;

@Slf4j @Getter @Setter
public class ObjectFilterClause extends FilterClause<ObjectFilterClause.Value> {
    @Getter @AllArgsConstructor
    public static class Value {
        public enum Kind {
            @SerializedName("VALUE")
            VALUE,
            @SerializedName("FIELD")
            FIELD,
            @SerializedName("CAST")
            CAST
        }
        @Expose
        Kind kind;
        @Expose
        String value;

        @Override
        public String toString() {
            return "{kind: " + kind + ", " + "value: " + value + "}";
        }
    }

    /**
     * Default Constructor. GSON recommended.
     */
    public ObjectFilterClause() {
        super();
    }

    /**
     *  Constructor takes a {@link StringFilterClause} object to construct from.
     *
     * @param stringFilterClause The {@link StringFilterClause} object tor construct from.
     */
    public ObjectFilterClause(StringFilterClause stringFilterClause) {
        this.operation = stringFilterClause.operation;
        this.field = stringFilterClause.field;
        this.patterns = stringFilterClause.patterns;
        List<String> stringValues = stringFilterClause.getValues();
        if (stringValues != null) {
            values = stringValues.stream().map(s -> s == null ? null : new Value(Value.Kind.VALUE, s)).collect(Collectors.toList());
        }
    }

    @Override
    public void configure(BulletConfig configuration) {
        if (operation == REGEX_LIKE) {
            patterns = values.stream().map(v -> FilterClause.compile(v.getValue())).filter(Objects::nonNull).collect(Collectors.toList());
        }
    }
}
