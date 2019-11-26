package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import static com.yahoo.bullet.querying.evaluators.Evaluator.NAryOperator;

public class NAryOperations {
    static NAryOperator ALL_MATCH = (evaluators, record) -> {
        for (Evaluator evaluator : evaluators) {
            // For now, type check before everything. Reconsider later
            TypedObject value = evaluator.evaluate(record);
            if (value.getType() != Type.BOOLEAN) {
                throw new UnsupportedOperationException("'AND' operands must have type BOOLEAN.");
            }
            if (!value.getBoolean()) {
                return new TypedObject(false);
            }
        }
        return new TypedObject(true);
    };

    static NAryOperator ANY_MATCH = (evaluators, record) -> {
        for (Evaluator evaluator : evaluators) {
            // For now, type check before everything. Reconsider later
            TypedObject value = evaluator.evaluate(record);
            if (value.getType() != Type.BOOLEAN) {
                throw new UnsupportedOperationException("'OR' operands must have type BOOLEAN.");
            }
            if (value.getBoolean()) {
                return new TypedObject(true);
            }
        }
        return new TypedObject(false);
    };

    static NAryOperator IF = (evaluators, record) -> {
        if (evaluators.size() != 3) {
            throw new UnsupportedOperationException("'IF' expects 3 operands.");
        }
        TypedObject condition = evaluators.get(0).evaluate(record);
        if (condition.getType() != Type.BOOLEAN) {
            throw new UnsupportedOperationException("'IF' first operand must have type BOOLEAN.");
        }
        return condition.getBoolean() ? evaluators.get(1).evaluate(record) : evaluators.get(2).evaluate(record);
    };
}
