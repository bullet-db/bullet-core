package com.yahoo.bullet.parsing.expressions;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;

import java.util.List;
import java.util.Optional;

/**
 * Null value. Not sure how useful this is since it seems like it's only used for "is null" and "not null."
 *
 * In that case, could just implement those as unary operations.
 */
public class LazyNull extends LazyExpression {

    public LazyNull() {
        type = Type.NULL;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }

    @Override
    public String getName() {
        return Type.NULL_EXPRESSION;
    }

    @Override
    public String toString() {
        return "{" + super.toString() + "}";
    }
}
