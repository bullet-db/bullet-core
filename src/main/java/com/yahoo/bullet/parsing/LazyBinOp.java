package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;

import java.util.List;
import java.util.Optional;

public class LazyBinOp extends LazyValue {
    @Expose
    private LazyValue left;
    private LazyValue right;
    private String operation;

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }

    @Override
    public String getName() {
        return left.getName() + " " + operation + " " + right.getName();
    }

    @Override
    public String toString() {
        return "{left: " + left + ", right: " + right + ", operation: " + operation + "}";
    }
}
