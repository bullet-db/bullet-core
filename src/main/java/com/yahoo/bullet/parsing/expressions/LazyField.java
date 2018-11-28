package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class LazyField extends LazyExpression {
    private static final BulletError LAZY_FIELD_REQUIRES_NON_NULL_FIELD = makeError("The field must not be null.", "Please provide a non-null field.");

    @Expose
    private String field;

    public LazyField() {
        field = null;
        type = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (field == null || field.isEmpty()) {
            return Optional.of(Collections.singletonList(LAZY_FIELD_REQUIRES_NON_NULL_FIELD));
        }
        return Optional.empty();
    }

    @Override
    public String getName() {
        return field;
    }

    @Override
    public String toString() {
        return "{field: " + field + ", " + super.toString() + "}";
    }
}
