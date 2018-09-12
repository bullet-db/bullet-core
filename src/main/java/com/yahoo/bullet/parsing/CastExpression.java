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
public class CastExpression extends Expression {
    public static final BulletError CAST_EXPRESSION_REQUIRES_VALID_EXPRESSION_ERROR =
            makeError("Casting needs a valid expression field", "Please add a valid expression.");
    public static final BulletError CAST_EXPRESSION_REQUIRES_PRIMITIVE_TYPE_ERROR =
            makeError("Casting needs a primitive type", "Please provide a primitive type.");

    @Expose
    private Expression expression;
    @Expose
    private Type type;

    /**
     * Default Constructor. GSON recommended.
     */
    public CastExpression() {
        super();
        expression = null;
        type = null;
    }

    @Override
    public String toString() {
        return "{" + super.toString() + ", expression" + ": " + expression + ", type: " + type + "}";
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (expression == null) {
            return Optional.of(Collections.singletonList(CAST_EXPRESSION_REQUIRES_VALID_EXPRESSION_ERROR));
        }
        Optional<List<BulletError>> errors = expression.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (type == null || !Type.PRIMITIVES.contains(type)) {
            return Optional.of(Collections.singletonList(CAST_EXPRESSION_REQUIRES_PRIMITIVE_TYPE_ERROR));
        }
        return Optional.empty();
    }
}
