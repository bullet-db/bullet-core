package com.yahoo.bullet.query;

import com.yahoo.bullet.query.expressions.Expression;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class Field {
    private String name;
    private Expression value;

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
