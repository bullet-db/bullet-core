/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.operations.FilterOperations;
import com.yahoo.bullet.typesystem.TypedObject;
import com.yahoo.bullet.querying.QueryRunner;
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

import static com.yahoo.bullet.common.Utilities.isEmpty;
import static com.yahoo.bullet.operations.FilterOperations.FilterType.REGEX_LIKE;
import static com.yahoo.bullet.operations.FilterOperations.RELATIONAL_OPERATORS;

@Slf4j @Getter @Setter
public class FilterClause extends Clause {
    @Expose
    private String field;
    @Expose
    private List<String> values;

    // An optimization to cache the compiled patterns per FilterClause rather than redoing it per record
    private List comparisons;

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
        if (isEmpty(values)) {
            return true;
        }
        TypedObject typed = new TypedObject(QueryRunner.extractField(field, record));
        return compare(operation, typed, values);
    }

    @Override
    public Optional<List<Error>> initialize() {
        comparisons = operation != REGEX_LIKE ? values : values.stream().map(FilterClause::safeCompile)
                                                                        .filter(Objects::nonNull)
                                                                        .collect(Collectors.toList());
        return super.initialize();
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", " + "field: " + field + ", " + "values: " + values + "}";
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
}
