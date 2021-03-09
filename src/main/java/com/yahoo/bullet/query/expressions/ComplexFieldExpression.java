/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.FieldEvaluator;
import lombok.Getter;

import java.util.Objects;

/**
 * An expression that requires a field name and a key expression for a list or map. A subkey expression can be
 * provided for a list of maps or map of maps.
 */
@Getter
public class ComplexFieldExpression extends Expression {
    private static final long serialVersionUID = 9156751450318196013L;

    private String field;
    private Expression key;
    private Expression subKey;

    /**
     * Constructor that creates a field expression with a list index or map key.
     *
     * @param field The non-null field to get.
     * @param key The non-null key to get from the field.
     */
    public ComplexFieldExpression(String field, Expression key) {
        this.field = Objects.requireNonNull(field);
        this.key = Objects.requireNonNull(key);
    }

    /**
     * Constructor that creates a field expression with a list index or map key and a map subkey.
     *
     * @param field The non-null field to get.
     * @param key The non-null key to get from the field.
     * @param subKey The non-null subkey to get from the field.key.
     */
    public ComplexFieldExpression(String field, Expression key, Expression subKey) {
        this.field = Objects.requireNonNull(field);
        this.key = Objects.requireNonNull(key);
        this.subKey = Objects.requireNonNull(subKey);
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
        if (!(obj instanceof ComplexFieldExpression)) {
            return false;
        }
        ComplexFieldExpression other = (ComplexFieldExpression) obj;
        return Objects.equals(field, other.field) &&
               Objects.equals(key, other.key) &&
               Objects.equals(subKey, other.subKey) &&
               type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, key, subKey, type);
    }

    @Override
    public String toString() {
        return "{field: " + field + ", key: " + key + ", subKey: " + subKey + ", " + super.toString() + "}";
    }
}
