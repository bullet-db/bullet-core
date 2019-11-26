package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.FieldEvaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
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
public class FieldExpression extends Expression {
    private static final BulletError FIELD_REQUIRES_NON_NULL_FIELD = makeError("The field must not be null.", "Please provide a non-null field.");
    private static final BulletError FIELD_REQUIRES_PRIMITIVE_TYPE = makeError("The type must be primitive (if specified).", "Please provide a primitive type or no type at all.");

    @Expose
    private String field;

    public FieldExpression() {
        field = null;
        type = null;
    }

    public FieldExpression(String field) {
        this.field = field;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (field == null || field.isEmpty()) {
            return Optional.of(Collections.singletonList(FIELD_REQUIRES_NON_NULL_FIELD));
        }
        if (type != null && !Type.PRIMITIVES.contains(type)) {
            return Optional.of(Collections.singletonList(FIELD_REQUIRES_PRIMITIVE_TYPE));
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
    public String toString() {
        return "{field: " + field + ", " + super.toString() + "}";
    }
}
