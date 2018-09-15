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

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter @Setter
public class LeafExpression extends Expression {
    public static final BulletError LEAF_EXPRESSION_REQUIRES_PRIMITIVE_TYPE_ERROR =
            makeError("The LeafExpression needs a value of primitive type", "Please provide a value of primitive type.");

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
    public String toString() {
        return "{" + super.toString() + ", value: " + value + "}";
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (value == null || (value.getKind() == Value.Kind.VALUE || value.getType() != null) && !Type.PRIMITIVES.contains(value.getType())) {
            return Optional.of(Collections.singletonList(LEAF_EXPRESSION_REQUIRES_PRIMITIVE_TYPE_ERROR));
        }
        return Optional.empty();
    }
}
