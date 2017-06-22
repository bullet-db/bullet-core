/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.operations.FilterOperations.LOGICAL_OPERATORS;

@Slf4j @Getter @Setter
public class LogicalClause extends Clause {
    @Expose
    List<Clause> clauses;

    /**
     * Default Constructor. GSON recommended.
     */
    public LogicalClause() {
        clauses = null;
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", " + "clauses: " + clauses + "}";
    }

    @Override
    public boolean check(BulletRecord record) {
        if (operation == null || clauses == null || clauses.isEmpty()) {
            return true;
        }
        return LOGICAL_OPERATORS.get(operation).test(record, clauses);
    }

    @Override
    public Optional<List<Error>> validate() {
        return Optional.empty();
    }
}

