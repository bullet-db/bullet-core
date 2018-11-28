package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.function.UnaryOperator;

public class UnaryOperations {

    static UnaryOperator<TypedObject> NOT = (value) -> {
        if (!Type.PRIMITIVES.contains(value.getType())) {
            throw new UnsupportedOperationException("'NOT' operand must have primitive type.");
        }
        Boolean negated = !((Boolean) value.forceCast(Type.BOOLEAN).getValue());
        return new TypedObject(negated);
    };

    static UnaryOperator<TypedObject> SIZE_OF = (value) -> {
        Type type = value.getType();
        if (type != Type.STRING && type != Type.LIST && type != Type.MAP) {
            throw new UnsupportedOperationException("'SIZEOF' operand must have type STRING, LIST, OR MAP");
        }
        return new TypedObject(value.size());
    };
}
