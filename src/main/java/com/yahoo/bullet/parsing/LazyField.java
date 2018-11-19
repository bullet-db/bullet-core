package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

public class LazyField extends LazyValue {
    private static final BulletError LAZY_FIELD_REQUIRES_NON_NULL_FIELD = makeError("The field must not be null.", "Please provide a non-null field.");

    @Expose
    private String field;

    @Override
    public Optional<List<BulletError>> initialize() {
        if (field == null || field.isEmpty())
            return Optional.of(Collections.singletonList(LAZY_FIELD_REQUIRES_NON_NULL_FIELD));
        return Optional.empty();
    }

    @Override
    public String getName() {
        return field;
    }

    @Override
    public String toString() {
        return "{field: " + field + "}";
    }
}
