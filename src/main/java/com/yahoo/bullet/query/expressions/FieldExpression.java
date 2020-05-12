/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.FieldEvaluator;
import lombok.Getter;

import java.util.Objects;

/**
 * An expression that requires a field name. An index or key can be provided for lists and maps as well as a subkey
 * for lists of maps and maps of maps.
 */
@Getter
public class FieldExpression extends Expression {
    private static final long serialVersionUID = -1659250076242321771L;
    private static final String DELIMITER = ".";

    private String field;
    private Integer index;
    private String key;
    private String subKey;

    public FieldExpression(String field) {
        this.field = Objects.requireNonNull(field);
    }

    public FieldExpression(String field, Integer index) {
        this.field = Objects.requireNonNull(field);
        this.index = Objects.requireNonNull(index);
    }

    public FieldExpression(String field, String key) {
        this.field = Objects.requireNonNull(field);
        this.key = Objects.requireNonNull(key);
    }

    public FieldExpression(String field, Integer index, String subKey) {
        this.field = Objects.requireNonNull(field);
        this.index = Objects.requireNonNull(index);
        this.subKey = Objects.requireNonNull(subKey);
    }

    public FieldExpression(String field, String key, String subKey) {
        this.field = Objects.requireNonNull(field);
        this.key = Objects.requireNonNull(key);
        this.subKey = Objects.requireNonNull(subKey);
    }

    public String getSimpleName() {
        if (index != null) {
            if (subKey != null) {
                return field + DELIMITER + index + DELIMITER + subKey;
            }
            return field + DELIMITER + index;
        }
        if (key != null) {
            if (subKey != null) {
                return field + DELIMITER + key + DELIMITER + subKey;
            }
            return field + DELIMITER + key;
        }
        return field;
    }

    @Override
    public Evaluator getEvaluator() {
        return new FieldEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof FieldExpression)) {
            return false;
        }
        FieldExpression other = (FieldExpression) obj;
        return Objects.equals(field, other.field) &&
               Objects.equals(index, other.index) &&
               Objects.equals(key, other.key) &&
               Objects.equals(subKey, other.subKey) &&
               type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, index, key, subKey, type);
    }

    @Override
    public String toString() {
        return "{field: " + field + ", index: " + index + ", key: " + key + ", subKey: " + subKey + ", " + super.toString() + "}";
    }
}
