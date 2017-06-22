/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.operations.FilterOperations;
import com.yahoo.bullet.operations.typesystem.TypedObject;
import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static com.yahoo.bullet.operations.FilterOperations.FilterType.REGEX_LIKE;
import static com.yahoo.bullet.operations.FilterOperations.RELATIONAL_OPERATORS;

@Slf4j @Getter @Setter
public class FilterClause extends Clause {
    @Expose
    private String field;
    @Expose
    private List<String> values;

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

    /**
     * Checks to see if this expression is satisfied.
     *
     * @param record The record to check the expression for.
     * @return true iff this expression is satisfied.
     */
    public boolean check(BulletRecord record) {
        if (operation == null || values == null || values.isEmpty()) {
            return true;
        }
        return compare(operation, Specification.extractField(field, record), values);
    }

    @SuppressWarnings("unchecked")
    private boolean compare(FilterOperations.FilterType operation, Object value, List<String> values) {
        TypedObject typed = new TypedObject(value);
        return operation == REGEX_LIKE ? RELATIONAL_OPERATORS.get(REGEX_LIKE).compare(typed, getValuesAsPatterns(values)) :
                                         RELATIONAL_OPERATORS.get(operation).compare(typed, values);
    }

    private static Pattern safeCompile(String value) {
        try {
            return Pattern.compile(value);
        } catch (PatternSyntaxException pse) {
            return null;
        }
    }

    private List<Pattern> getValuesAsPatterns(List<String> values) {
        if (patterns == null) {
            // Remove all null patterns
            patterns = values.stream().map(FilterClause::safeCompile).filter(Objects::nonNull).collect(Collectors.toList());
        }
        return patterns;
    }

    @Override
    public Optional<List<Error>> validate() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", " + "field: " + field + ", " + "values: " + values + "}";
    }
}
