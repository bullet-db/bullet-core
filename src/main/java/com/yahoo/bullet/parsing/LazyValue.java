package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.typesystem.Type;

public abstract class LazyValue implements Configurable, Initializable {
    @Expose
    protected Type type;

    public abstract String getName();
}
