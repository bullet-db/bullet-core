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
    private Expression variableKey;
    private Expression variableSubKey;

    /**
     * Constructor that creates a field expression.
     *
     * @param field The non-null field to get.
     */
    public FieldExpression(String field) {
        this.field = Objects.requireNonNull(field);
    }

    /**
     * Constructor that creates a field expression with a list index.
     *
     * @param field The non-null field to get.
     * @param index The non-null index to get from the field.
     */
    public FieldExpression(String field, Integer index) {
        this.field = Objects.requireNonNull(field);
        this.index = Objects.requireNonNull(index);
    }

    /**
     * Constructor that creates a field expression with a map key.
     *
     * @param field The non-null field to get.
     * @param key The non-null key to get from the field.
     */
    public FieldExpression(String field, String key) {
        this.field = Objects.requireNonNull(field);
        this.key = Objects.requireNonNull(key);
    }

    /**
     * Constructor that creates a field expression with a variable list index or map key.
     *
     * @param field The non-null field to get.
     * @param variableKey The non-null variable key to get from the field.
     */
    public FieldExpression(String field, Expression variableKey) {
        this.field = Objects.requireNonNull(field);
        this.variableKey = Objects.requireNonNull(variableKey);
    }

    /**
     * Constructor that creates a field expression with a list index and a map subkey.
     *
     * @param field The non-null field to get.
     * @param index The non-null index to get from the field.
     * @param subKey The non-null subkey to get from the field[index].
     */
    public FieldExpression(String field, Integer index, String subKey) {
        this.field = Objects.requireNonNull(field);
        this.index = Objects.requireNonNull(index);
        this.subKey = Objects.requireNonNull(subKey);
    }

    /**
     * Constructor that creates a field expression with a map key and a map subkey.
     *
     * @param field The non-null field to get.
     * @param key The non-null key to get from the field.
     * @param subKey The non-null subkey to get from the field.key.
     */
    public FieldExpression(String field, String key, String subKey) {
        this.field = Objects.requireNonNull(field);
        this.key = Objects.requireNonNull(key);
        this.subKey = Objects.requireNonNull(subKey);
    }

    /**
     * Constructor that creates a field expression with a variable list index or map key and a map subkey.
     *
     * @param field The non-null field to get.
     * @param variableKey The non-null variable key to get from the field.
     * @param subKey The non-null subkey to get from the field[key].
     */
    public FieldExpression(String field, Expression variableKey, String subKey) {
        this.field = Objects.requireNonNull(field);
        this.variableKey = Objects.requireNonNull(variableKey);
        this.subKey = Objects.requireNonNull(subKey);
    }

    /**
     * Constructor that creates a field expression with a list index and a variable map subkey.
     *
     * @param field The non-null field to get.
     * @param index The non-null index to get from the field.
     * @param variableSubKey The non-null variable subkey to get from the field[index].
     */
    public FieldExpression(String field, Integer index, Expression variableSubKey) {
        this.field = Objects.requireNonNull(field);
        this.index = Objects.requireNonNull(index);
        this.variableSubKey = Objects.requireNonNull(variableSubKey);
    }

    /**
     * Constructor that creates a field expression with a map key and a variable map subkey.
     *
     * @param field The non-null field to get.
     * @param key The non-null key to get from the field.
     * @param variableSubKey The non-null variable subkey to get from the field.key.
     */
    public FieldExpression(String field, String key, Expression variableSubKey) {
        this.field = Objects.requireNonNull(field);
        this.key = Objects.requireNonNull(key);
        this.variableSubKey = Objects.requireNonNull(variableSubKey);
    }

    /**
     * Constructor that creates a field expression with map key and a map subkey.
     *
     * @param field The non-null field to get.
     * @param variableKey The non-null variable key to get from the field.
     * @param variableSubKey The non-null variable subkey to get from the field[key].
     */
    public FieldExpression(String field, Expression variableKey, Expression variableSubKey) {
        this.field = Objects.requireNonNull(field);
        this.variableKey = Objects.requireNonNull(variableKey);
        this.variableSubKey = Objects.requireNonNull(variableSubKey);
    }

    /**
     * Constructor that creates a subfield expression from a field expression and a list index.
     *
     * @param other The non-null field expression to get a subfield from.
     * @param index The non-null index to get from the field expression.
     * @throws IllegalArgumentException if the field expression has an index or key and therefore cannot have another index.
     */
    public FieldExpression(FieldExpression other, Integer index) {
        Objects.requireNonNull(other);
        Objects.requireNonNull(index);
        if (other.index != null || other.key != null || other.variableKey != null || other.subKey != null || other.variableSubKey != null) {
            throw new IllegalArgumentException();
        }
        this.field = other.field;
        this.index = index;
    }

    /**
     * Constructor that creates a subfield expression from a field expression and a map key.
     *
     * @param other The non-null field expression to get a subfield from.
     * @param key The non-null key to get from the field expression.
     * @throws IllegalArgumentException if the field expression has a subkey and therefore cannot have another key.
     */
    public FieldExpression(FieldExpression other, String key) {
        Objects.requireNonNull(other);
        Objects.requireNonNull(key);
        if (other.subKey != null || other.variableSubKey != null) {
            throw new IllegalArgumentException();
        } else if (other.index != null) {
            this.index = other.index;
            this.subKey = key;
        } else if (other.key != null) {
            this.key = other.key;
            this.subKey = key;
        } else if (other.variableKey != null) {
            this.variableKey = other.variableKey;
            this.subKey = key;
        } else {
            this.key = key;
        }
        this.field = other.field;
    }

    /**
     * Constructor that creates a subfield expression from a field expression and a variable key.
     *
     * @param other The non-null field expression to get a subfield from.
     * @param variableKey The non-null variable key to get from the field expression.
     * @throws IllegalArgumentException if the field expression has a subkey and therefore cannot have another key.
     */
    public FieldExpression(FieldExpression other, Expression variableKey) {
        Objects.requireNonNull(other);
        Objects.requireNonNull(variableKey);
        if (other.subKey != null || other.variableSubKey != null) {
            throw new IllegalArgumentException();
        } else if (other.index != null) {
            this.index = other.index;
            this.variableSubKey = variableKey;
        } else if (other.key != null) {
            this.key = other.key;
            this.variableSubKey = variableKey;
        } else if (other.variableKey != null) {
            this.variableKey = other.variableKey;
            this.variableSubKey = variableKey;
        } else {
            this.variableKey = variableKey;
        }
        this.field = other.field;
    }

    /**
     * Gets the name of this field expression formatted with delimiters for any index and/or keys. This name is used in
     * the {@link com.yahoo.bullet.querying.partitioning.SimpleEqualityPartitioner}.
     *
     * @return The name of this field expression.
     */
    public String getName() {
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
               Objects.equals(variableKey, other.variableKey) &&
               Objects.equals(variableSubKey, other.variableSubKey) &&
               type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, index, key, subKey, variableKey, variableSubKey, type);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{field: ").append(field);
        if (index != null) {
            builder.append(", index: ").append(index);
        } else if (key != null) {
            builder.append(", key: ").append(key);
        } else if (variableKey != null) {
            builder.append(", variableKey: ").append(variableKey);
        }
        if (subKey != null) {
            builder.append(", subKey: ").append(subKey);
        } else if (variableSubKey != null) {
            builder.append(", variableSubKey: ").append(variableSubKey);
        }
        builder.append(", ").append(super.toString()).append("}");
        return builder.toString();
    }
}
