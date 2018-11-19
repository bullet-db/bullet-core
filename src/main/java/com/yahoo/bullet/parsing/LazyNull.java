package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;

import java.util.List;
import java.util.Optional;

public class LazyNull extends LazyValue {
    @Override
    public Optional<List<BulletError>> initialize() {
        type = Type.NULL;
        return Optional.empty();
    }

    @Override
    public String getName() {
        return "null";
    }

    @Override
    public String toString() {
        return "{}";
    }
}
