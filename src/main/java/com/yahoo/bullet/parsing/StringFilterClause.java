/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j @Getter @Setter
public class StringFilterClause extends FilterClause<String> {
    /**
     * Default Constructor. GSON recommended.
     */
    public StringFilterClause() {
        super();
    }

    @Override
    public String getValue(String value) {
        return value;
    }
}
