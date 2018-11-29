package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class LazyList extends LazyExpression {
    private static final BulletError LAZY_LIST_REQUIRES_NON_NULL_LIST = makeError("The values list must not be null.", "Please provide a values list.");
    private static final BulletError LAZY_LIST_REQUIRES_PRIMITIVE_TYPE = makeError("The type must be primitive.", "Please provide a primitive type.");
    private static final String DELIMITER = ", ";

    @Expose
    private List<LazyExpression> values;

    public LazyList() {
        values = null;
        type = null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (values == null) {
            return Optional.of(Collections.singletonList(LAZY_LIST_REQUIRES_NON_NULL_LIST));
        }
        if (!Type.PRIMITIVES.contains(type)) {
            return Optional.of(Collections.singletonList(LAZY_LIST_REQUIRES_PRIMITIVE_TYPE));
        }
        List<BulletError> errors = new ArrayList<>();
        values.forEach(values -> values.initialize().ifPresent(errors::addAll));
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    @Override
    public String getName() {
        return "[" + values.stream().map(LazyExpression::getName).collect(Collectors.joining(DELIMITER)) + "]";
    }

    @Override
    public String toString() {
        return "{values: " + values + ", " + super.toString() + "}";
    }
}
