package com.yahoo.bullet.parsing.expressions;

import com.yahoo.bullet.common.BulletError;

import java.util.List;
import java.util.Optional;

public class LazyMap extends LazyExpression {

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }

    @Override
    public String getName() {
        return null;
    }
}
