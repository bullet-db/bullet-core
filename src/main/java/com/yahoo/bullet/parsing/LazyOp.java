package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletError;

import java.util.List;
import java.util.Optional;

public class LazyOp extends LazyValue {


    @Override
    public String getName() {
        return null;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }
}
