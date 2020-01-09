/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.parsing.expressions.Expression;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Slf4j @Getter @Setter
public class Projection implements Configurable, Initializable {
    @Getter @Setter @NoArgsConstructor @AllArgsConstructor
    public static class Field {
        @Expose
        private String name;
        @Expose
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

    @Expose
    private List<Field> fields;

    /**
     * Default constructor. GSON recommended.
     */
    public Projection() {
        fields = null;
    }

    public Projection(List<Field> fields) {
        this.fields = fields;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        List<BulletError> errors = new ArrayList<>();
        fields.stream().map(Field::getValue).forEach(f -> f.initialize().ifPresent(errors::addAll));
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }

    @Override
    public String toString() {
        return "{fields: " + fields + "}";
    }
}
