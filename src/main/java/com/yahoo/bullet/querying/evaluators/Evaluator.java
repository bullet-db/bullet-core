package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Evaluators do the work of expressions.
 *
 * Each expression is built into a corresponding evaluator. Note, evaluators are constructed after a query has been
 * initialized so assume all expressions are valid.
 *
 * Evaluators are evaluated given a BulletRecord and will throw exceptions on any errors. These errors are virtually all
 * from some form of type check.
 *
 * Also, note the type cast in evaluator. For primitives, this acts as how you think it would, but for lists and maps, it
 * will cast their elements/values.
 */
public abstract class Evaluator {
    protected Type type;

    static final Map<Operation, BinaryOperator<TypedObject>> BINARY_OPERATORS = new EnumMap<>(Operation.class);
    static final Map<Operation, UnaryOperator<TypedObject>> UNARY_OPERATORS = new EnumMap<>(Operation.class);

    static {
        BINARY_OPERATORS.put(Operation.ADD, BinaryOperations.ADD);
        BINARY_OPERATORS.put(Operation.SUB, BinaryOperations.SUB);
        BINARY_OPERATORS.put(Operation.MUL, BinaryOperations.MUL);
        BINARY_OPERATORS.put(Operation.DIV, BinaryOperations.DIV);
        BINARY_OPERATORS.put(Operation.EQUALS, BinaryOperations.EQUALS);
        BINARY_OPERATORS.put(Operation.NOT_EQUALS, BinaryOperations.NOT_EQUALS);
        BINARY_OPERATORS.put(Operation.GREATER_THAN, BinaryOperations.GREATER_THAN);
        BINARY_OPERATORS.put(Operation.LESS_THAN, BinaryOperations.LESS_THAN);
        BINARY_OPERATORS.put(Operation.GREATER_THAN_OR_EQUALS, BinaryOperations.GREATER_THAN_OR_EQUALS);
        BINARY_OPERATORS.put(Operation.LESS_THAN_OR_EQUALS, BinaryOperations.LESS_THAN_OR_EQUALS);
        BINARY_OPERATORS.put(Operation.REGEX_LIKE, BinaryOperations.REGEX_LIKE);
        BINARY_OPERATORS.put(Operation.SIZE_IS, BinaryOperations.SIZE_IS);
        BINARY_OPERATORS.put(Operation.CONTAINS_KEY, BinaryOperations.CONTAINS_KEY);
        BINARY_OPERATORS.put(Operation.CONTAINS_VALUE, BinaryOperations.CONTAINS_VALUE);
        BINARY_OPERATORS.put(Operation.AND, BinaryOperations.AND);
        BINARY_OPERATORS.put(Operation.OR, BinaryOperations.OR);
        BINARY_OPERATORS.put(Operation.XOR, BinaryOperations.XOR);
        BINARY_OPERATORS.put(Operation.FILTER, BinaryOperations.FILTER);
        UNARY_OPERATORS.put(Operation.NOT, UnaryOperations.NOT);
        UNARY_OPERATORS.put(Operation.SIZE_OF, UnaryOperations.SIZE_OF);
        UNARY_OPERATORS.put(Operation.IS_NULL, UnaryOperations.IS_NULL);
        UNARY_OPERATORS.put(Operation.IS_NOT_NULL, UnaryOperations.IS_NOT_NULL);
    }

    Evaluator(Expression expression) {
        type = expression.getType();
    }

    public abstract TypedObject evaluate(BulletRecord record);

    protected TypedObject cast(TypedObject object) {
        if (type == null || ((object.getType() == Type.LIST || object.getType() == Type.MAP) && object.getPrimitiveType() == type)) {
            return object;
        }
        if (object.getType() == Type.LIST) {
            List<Object> objects = object.getList();
            return new TypedObject(Type.LIST, objects.stream().map(o -> TypedObject.typeCastFromObject(type, o).getValue()).collect(Collectors.toList()));
        }
        if (object.getType() == Type.MAP) {
            Map<String, Object> map = object.getMap();
            Map<String, Object> newMap = new HashMap<>();
            map.forEach((key, value) -> newMap.put(key, TypedObject.typeCastFromObject(type, value).getValue()));
            return new TypedObject(Type.MAP, newMap);
        }
        return object.forceCast(type);
    }

    /**
     * What is polymorphism?
     *
     * @param expression
     * @return
     */
    public static Evaluator build(Expression expression) {
        return expression != null ? expression.getEvaluator() : null;
    }
}
