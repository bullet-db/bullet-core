package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.parsing.expressions.Expression;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class Field {
    @Expose
    private String name;
    @Expose
    private Expression value;
}
