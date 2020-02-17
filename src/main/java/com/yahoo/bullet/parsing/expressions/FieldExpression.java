/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.FieldEvaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * An expression that takes a field name. A primitive type can be provided.
 *
 * Note, a type should NOT be provided if the field is expected to be a List of Maps or a Map of Maps since only
 * primitive type-casting is supported.
 *
 * For example, if a field is extracted as a list of boolean maps and the type specified is boolean, then the evaluator
 * will try to cast those boolean maps to boolean objects (and fail).
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FieldExpression extends Expression {
    private static final BulletError FIELD_REQUIRES_NON_NULL_FIELD = makeError("The field must not be null.", "Please provide a non-null field.");
    private static final BulletError SUB_FIELD_REQUIRES_ONLY_INDEX_OR_KEY = makeError("The index and key cannot both be specified.", "Please provide one or the other.");
    private static final BulletError SUB_SUB_FIELD_REQUIRES_INDEX_OR_KEY = makeError("The index or key must be specified for subkey.", "Plesae provide an index or key or remove the subkey.");
    private static final BulletError SUB_SUB_FIELD_REQUIRES_TYPE_TO_BE_PRIMITIVE_TYPE = makeError("The type must be primitive for subsubfield (if specified).", "Please provide a primitive type or no type at all.");
    private static final BulletError SUB_SUB_FIELD_REQUIRES_PRIMITIVE_TYPE_TO_BE_NULL = makeError("The primitive type must be null for subsubfield.", "Please set the primitive type to null.");
    private static final BulletError SUB_FIELD_REQUIRES_TYPE_TO_BE_PRIMITIVE_OR_MAP = makeError("The type must be primitive or map for subfield (if specified).", "Please provide a primitive type, map type, or no type at all.");
    private static final BulletError SUB_FIELD_REQUIRES_PRIMITIVE_TYPE_FOR_MAP = makeError("The primitive type must be primitive for subfield if map type.", "Please provide a primitive type for primitive type.");
    private static final BulletError SUB_FIELD_REQUIRES_PRIMITIVE_TYPE_TO_BE_NULL_IF_NOT_MAP = makeError("The primitive type must be null for subfield if not map type.", "Please set the primitive type to null.");
    private static final BulletError FIELD_REQUIRES_PRIMITIVE_TYPE_FOR_COLLECTION = makeError("The primitive type must be specified for field if list/map type.", "Please provide a primitive type.");
    private static final BulletError FIELD_REQUIRES_PRIMITIVE_TYPE_TO_BE_NULL_IF_NOT_COLLECTION = makeError("The primitive type must null for field if not list/map type.", "Please set the primitive type to null.");

    @Expose
    private String field;
    @Expose
    private Integer index;
    @Expose
    private String key;
    @Expose
    private String subKey;

    public FieldExpression(String field) {
        this(field, null, null, null);
    }

    public FieldExpression(String field, Integer index) {
        this(field, index, null, null);
    }

    public FieldExpression(String field, String key) {
        this(field, null, key, null);
    }

    public FieldExpression(String field, Integer index, String subKey) {
        this(field, index, null, subKey);
    }

    public FieldExpression(String field, String key, String subKey) {
        this(field, null, key, subKey);
    }

    public FieldExpression(String field, Integer index, String key, String subKey, Type type) {
        this(field, index, key, subKey);
        this.type = type;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (field == null || field.isEmpty()) {
            return Optional.of(Collections.singletonList(FIELD_REQUIRES_NON_NULL_FIELD));
        }
        if (index != null && key != null) {
            return Optional.of(Collections.singletonList(SUB_FIELD_REQUIRES_ONLY_INDEX_OR_KEY));
        }
        if (subKey != null) {
            if (index == null && key == null) {
                return Optional.of(Collections.singletonList(SUB_SUB_FIELD_REQUIRES_INDEX_OR_KEY));
            }
            if (type != null && !Type.isPrimitive(type)) {
                return Optional.of(Collections.singletonList(SUB_SUB_FIELD_REQUIRES_TYPE_TO_BE_PRIMITIVE_TYPE));
            }
        } else if (index != null || key != null) {
            if (type != null && !Type.isPrimitive(type) && !Type.isPrimitiveMap(type)) {
                return Optional.of(Collections.singletonList(SUB_FIELD_REQUIRES_TYPE_TO_BE_PRIMITIVE_OR_MAP));
            }
        }
        return Optional.empty();
    }

    @Override
    public String getName() {
        if (index != null) {
            if (subKey != null) {
                return field + "[" + index + "]." + subKey;
            }
            return field + "[" + index + "]";
        }
        if (key != null) {
            if (subKey != null) {
                return field + "." + key + "." + subKey;
            }
            return field + "." + key;
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
               Objects.equals(subKey, other.subKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, index, key, subKey);
    }

    @Override
    public String toString() {
        return "{field: " + field + ", index: " + index + ", key: " + key + ", subKey: " + subKey + ", " + super.toString() + "}";
    }
}
