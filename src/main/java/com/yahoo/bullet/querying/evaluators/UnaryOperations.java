package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import static com.yahoo.bullet.querying.evaluators.Evaluator.UnaryOperator;

/**
 * Unary operations used by UnaryEvaluator.
 */
public class UnaryOperations {
    static UnaryOperator NOT = (evaluator, record) -> {
        TypedObject value = evaluator.evaluate(record);
        // TODO push to expressions type-checking
        /*if (!Type.PRIMITIVES.contains(value.getType())) {
            throw new UnsupportedOperationException("'NOT' operand must have primitive type.");
        }*/
        Boolean negated = !((Boolean) value.forceCast(Type.BOOLEAN).getValue());
        return new TypedObject(negated);
    };

    static UnaryOperator SIZE_OF = (evaluator, record) -> {
        TypedObject value = evaluator.evaluate(record);
        Type type = value.getType();
        // TODO push to expressions type-checking
        /*if (type != Type.STRING && type != Type.LIST && type != Type.MAP) {
            throw new UnsupportedOperationException("'SIZEOF' operand must have type STRING, LIST, OR MAP.");
        }*/
        return new TypedObject(value.size());
    };

    static UnaryOperator IS_NULL = (evaluator, record) ->
            new TypedObject(evaluator.evaluate(record).getType() == Type.NULL);

    static UnaryOperator IS_NOT_NULL = (evaluator, record) ->
            new TypedObject(evaluator.evaluate(record).getType() != Type.NULL);
/*
    static UnaryOperator ALL_MATCH = (evaluator, record) -> {
        TypedObject value = evaluator.evaluate(record);
        if (value.getType() != Type.LIST && value.getPrimitiveType() != Type.BOOLEAN) {
            throw new UnsupportedOperationException("'AND' operand must have type LIST with BOOLEAN primitive type.");
        }
        return new TypedObject(value.getList().stream().allMatch(o -> (Boolean) o));
    };

    static UnaryOperator ANY_MATCH = (evaluator, record) -> {
        TypedObject value = evaluator.evaluate(record);
        if (value.getType() != Type.LIST && value.getPrimitiveType() != Type.BOOLEAN) {
            throw new UnsupportedOperationException("'OR' operand must have type LIST with BOOLEAN primitive type.");
        }
        return new TypedObject(value.getList().stream().anyMatch(o -> (Boolean) o));
    };

    static UnaryOperator ADD = (evaluator, record) -> {
        TypedObject value = evaluator.evaluate(record);
        if (value.getType() != Type.LIST) {
            throw new UnsupportedOperationException("'ADD' operand must have type LIST.");
        }
        switch (value.getPrimitiveType()) {
            case INTEGER:
                return new TypedObject(value.getList().stream().reduce(0, (i1, i2) -> i1 + (Integer) i2, (i1, i2) -> i1 + i2));
            case LONG:
                return new TypedObject(value.getList().stream().reduce(0L, (l1, l2) -> l1 + (Long) l2, (l1, l2) -> l1 + l2));
            case FLOAT:
                return new TypedObject(value.getList().stream().reduce(0.0f, (f1, f2) -> f1 + (Float) f2, (f1, f2) -> f1 + f2));
            case DOUBLE:
                return new TypedObject(value.getList().stream().reduce(0.0, (d1, d2) -> d1 + (Double) d2, (d1, d2) -> d1 + d2));
        }
        throw new UnsupportedOperationException("'ADD' operand must have type LIST with numeric primitive type.");
    };
*/
}
