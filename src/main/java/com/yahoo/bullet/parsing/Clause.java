/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.operations.FilterOperations.FilterType;
import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j @Getter @Setter
public abstract class Clause implements Configurable, Validatable {
    @Expose
    @SerializedName(OPERATION_FIELD)
    protected FilterType operation;

    public static final String OPERATION_FIELD = "operation";

    /**
     * Check this clause against this record. Return true iff the clause is satisfied.
     *
     * @param record The {@link BulletRecord} to check this clause against.
     * @return a boolean denoting if the check failed or passed.
     */
    public abstract boolean check(BulletRecord record);

    @Override
    public String toString() {
        return OPERATION_FIELD + ": " + operation;
    }
}

