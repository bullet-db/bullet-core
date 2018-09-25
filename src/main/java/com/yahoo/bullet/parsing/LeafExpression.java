/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter @Setter
public class LeafExpression extends Expression {
    public static final BulletError LEAF_EXPRESSION_REQUIRES_VALUE_FIELD_ERROR =
            makeError("The expression needs a value field", "Please provide a value field.");
    public static final BulletError LEAF_EXPRESSION_REQUIRES_PRIMITIVE_TYPE_ERROR =
            makeError("The expression needs type set to only a primitive type", "Please provide a primitive type.");
    public static final BulletError LEAF_EXPRESSION_VALUE_KIND_REQUIRES_TYPE_ERROR =
            makeError("The expression needs a non-null type if the kind of value is 'VALUE'", "Please provide a non-null type.");

    @Expose
    private Value value;

    /**
     * Default Constructor. GSON recommended.
     */
    public LeafExpression() {
        super();
        value = null;
    }

    @Override
    public Set<String> getRequiredFields() {
        return value.getKind() == Value.Kind.FIELD ? Collections.singleton(value.getValue()) : Collections.emptySet();
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", value: " + value + "}";
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (value == null) {
            return Optional.of(Collections.singletonList(LEAF_EXPRESSION_REQUIRES_VALUE_FIELD_ERROR));
        }
        Optional<List<BulletError>> errors = value.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (value.getType() != null && !Type.PRIMITIVES.contains(value.getType())) {
            return Optional.of(Collections.singletonList(LEAF_EXPRESSION_REQUIRES_PRIMITIVE_TYPE_ERROR));
        }
        if (value.getKind() == Value.Kind.VALUE && value.getType() == null) {
            return Optional.of(Collections.singletonList(LEAF_EXPRESSION_VALUE_KIND_REQUIRES_TYPE_ERROR));
        }
        return Optional.empty();
    }
}
