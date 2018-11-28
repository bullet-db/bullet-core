package com.yahoo.bullet.parsing.expressions;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;

import java.util.List;
import java.util.Optional;

public class LazyNull extends LazyExpression {

    public LazyNull() {
        type = Type.NULL;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        type = Type.NULL;
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
