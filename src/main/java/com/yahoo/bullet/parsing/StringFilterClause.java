/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.stream.Collectors;

import static com.yahoo.bullet.parsing.Clause.Operation.REGEX_LIKE;

@Slf4j @Getter @Setter
public class StringFilterClause extends FilterClause<String> {
    /**
     * Default Constructor. GSON recommended.
     */
    public StringFilterClause() {
        super();
    }

    @Override
    public void configure(BulletConfig configuration) {
        if (operation == REGEX_LIKE) {
            patterns = values.stream().map(FilterClause::compile).filter(Objects::nonNull).collect(Collectors.toList());
        }
    }
}
