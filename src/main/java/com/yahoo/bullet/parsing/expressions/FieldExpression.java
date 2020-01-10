/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.FieldEvaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
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
public class FieldExpression extends Expression {
    private static final BulletError FIELD_REQUIRES_NON_NULL_FIELD = makeError("The field must not be null.", "Please provide a non-null field.");
    private static final BulletError FIELD_REQUIRES_PRIMITIVE_TYPE = makeError("The type must be primitive (if specified).", "Please provide a primitive type or no type at all.");

    @Expose
    private String field;
    @Expose
    private Integer index;
    @Expose
    private String key;
    @Expose
    private String subKey;

    public FieldExpression() {
        field = null;
        key = null;
        subKey = null;
        type = null;
        primitiveType = null;
    }

    public FieldExpression(String field, Integer index, String key, String subKey, Type type, Type primitiveType) {
        this.field = field;
        this.index = index;
        this.key = key;
        this.subKey = subKey;
        this.type = type;
        this.primitiveType = primitiveType;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (field == null || field.isEmpty()) {
            return Optional.of(Collections.singletonList(FIELD_REQUIRES_NON_NULL_FIELD));
        }
        if (type != null && !Type.PRIMITIVES.contains(type)) {
            return Optional.of(Collections.singletonList(FIELD_REQUIRES_PRIMITIVE_TYPE));
        }


        if (index != null && key != null) {
            // can't have both
        }

        // if subkey is not null, then type must be primitive and primitivetype should be null
        if (subKey != null) {
            if (index == null && key == null) {
                // must have one
            }
            if (!Type.PRIMITIVES.contains(type)) {
                // type must be primitive
            }
            if (primitiveType != null) {
                // primitiveType must be null
            }

        } else if (index != null || key != null) {
            if (!Type.PRIMITIVES.contains(type) && type != Type.MAP) {
                // type has to be primitive or map
            }
            if (type == Type.MAP) {
                if (!Type.PRIMITIVES.contains(primitiveType)) {
                    // primitivetype needs to be primitive type
                }
            } else if (primitiveType != null) {
                // primitivetype has to be null if type is primitive
            }
        } else {
            // type can be anything
            if (Type.COLLECTIONS.contains(type)) {
                if (!Type.PRIMITIVES.contains(primitiveType)) {
                    // primitivetype has to be primitive if type is list/map
                }
            } else if (primitiveType != null) {
                // primitive type has to be null if type isn't list/map
            }
        }


        return Optional.empty();
    }

    @Override
    public String getName() {
        return field;
    }

    @Override
    public Evaluator getEvaluator() {
        return new FieldEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
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
