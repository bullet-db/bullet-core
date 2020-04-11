package com.yahoo.bullet.query;

import com.yahoo.bullet.query.expressions.Expression;
import lombok.Getter;

import java.util.Objects;

@Getter
public class Field {
    private String name;
    private Expression value;

    public Field(String name, Expression value) {
        this.name = Objects.requireNonNull(name);
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Field)) {
            return false;
        }
        Field other = (Field) obj;
        return Objects.equals(name, other.name) && Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }
}
