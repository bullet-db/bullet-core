/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.JsonParseException;
import com.yahoo.bullet.parsing.Error;
import com.yahoo.bullet.parsing.Parser;
import com.yahoo.bullet.parsing.ParsingException;
import com.yahoo.bullet.parsing.Specification;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractQuery<T, R> implements Query<T, R> {
    protected String queryString;
    protected int duration;
    protected Specification specification;
    @Getter
    protected long startTime;

    /**
     * Constructor that takes a String representation of the query and a configuration to use.
     *
     * @param queryString The query as a string.
     * @param configuration The configuration to use.
     * @throws ParsingException if there was an issue.
     */
    public AbstractQuery(String queryString, Map configuration) throws JsonParseException, ParsingException {
        this.queryString = queryString;
        specification = Parser.parse(queryString, configuration);
        Optional<List<Error>> errors = specification.validate();
        if (errors.isPresent()) {
            throw new ParsingException(errors.get());
        }
        duration = specification.getDuration();
        startTime = System.currentTimeMillis();
    }

    /**
     * Returns true iff the query has expired.
     *
     * @return boolean denoting if query has expired.
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > startTime + duration;
    }

    @Override
    public String toString() {
        return queryString;
    }
}
