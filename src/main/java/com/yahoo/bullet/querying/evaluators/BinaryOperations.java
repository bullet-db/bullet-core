package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.yahoo.bullet.querying.evaluators.Evaluator.BinaryOperator;

/**
 * Binary operations used by BinaryEvaluator.
 */
public class BinaryOperations {
    static BinaryOperator ADD = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getResultType(leftValue.getType(), rightValue.getType());
        switch (type) {
            case INTEGER:
                return new TypedObject(Type.INTEGER, leftValue.getInteger() + rightValue.getInteger());
            case LONG:
                return new TypedObject(Type.LONG, leftValue.getLong() + rightValue.getLong());
            case FLOAT:
                return new TypedObject(Type.FLOAT, leftValue.getFloat() + rightValue.getFloat());
            case DOUBLE:
                return new TypedObject(Type.DOUBLE, leftValue.getDouble() + rightValue.getDouble());
        }
        throw new UnsupportedOperationException("'+' operands must have numeric type.");
    };

    static BinaryOperator SUB = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getResultType(leftValue.getType(), rightValue.getType());
        switch (type) {
            case INTEGER:
                return new TypedObject(Type.INTEGER, leftValue.getInteger() - rightValue.getInteger());
            case LONG:
                return new TypedObject(Type.LONG, leftValue.getLong() - rightValue.getLong());
            case FLOAT:
                return new TypedObject(Type.FLOAT, leftValue.getFloat() - rightValue.getFloat());
            case DOUBLE:
                return new TypedObject(Type.DOUBLE, leftValue.getDouble() - rightValue.getDouble());
        }
        throw new UnsupportedOperationException("'-' operands must have numeric type.");
    };

    static BinaryOperator MUL = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getResultType(leftValue.getType(), rightValue.getType());
        switch (type) {
            case INTEGER:
                return new TypedObject(Type.INTEGER, leftValue.getInteger() * rightValue.getInteger());
            case LONG:
                return new TypedObject(Type.LONG, leftValue.getLong() * rightValue.getLong());
            case FLOAT:
                return new TypedObject(Type.FLOAT, leftValue.getFloat() * rightValue.getFloat());
            case DOUBLE:
                return new TypedObject(Type.DOUBLE, leftValue.getDouble() * rightValue.getDouble());
        }
        throw new UnsupportedOperationException("'*' operands must have numeric type.");
    };

    static BinaryOperator DIV = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getResultType(leftValue.getType(), rightValue.getType());
        switch (type) {
            case INTEGER:
                return new TypedObject(Type.INTEGER, leftValue.getInteger() / rightValue.getInteger());
            case LONG:
                return new TypedObject(Type.LONG, leftValue.getLong() / rightValue.getLong());
            case FLOAT:
                return new TypedObject(Type.FLOAT, leftValue.getFloat() / rightValue.getFloat());
            case DOUBLE:
                return new TypedObject(Type.DOUBLE, leftValue.getDouble() / rightValue.getDouble());
        }
        throw new UnsupportedOperationException("'/' operands must have numeric type.");
    };

    static BinaryOperator EQUALS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type leftType = leftValue.getType();
        Type rightType = rightValue.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'==' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(leftValue.equalTo(rightValue));
        }
        if (rightType == Type.LIST && leftType == rightValue.getPrimitiveType()) {
            return new TypedObject(rightValue.getList().stream().anyMatch(leftValue::equalTo));
        }
        throw new UnsupportedOperationException("'==' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator NOT_EQUALS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type leftType = leftValue.getType();
        Type rightType = rightValue.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'!=' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(!leftValue.equalTo(rightValue));
        }
        if (rightType == Type.LIST && leftType == rightValue.getPrimitiveType()) {
            return new TypedObject(rightValue.getList().stream().noneMatch(leftValue::equalTo));
        }
        throw new UnsupportedOperationException("'!=' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator GREATER_THAN = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type leftType = leftValue.getType();
        Type rightType = rightValue.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'>' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(leftValue.compareTo(rightValue) > 0);
        }
        if (rightType == Type.LIST && leftType == rightValue.getPrimitiveType()) {
            return new TypedObject(rightValue.getList().stream().anyMatch(o -> leftValue.compareTo(new TypedObject(o)) > 0));
        }
        throw new UnsupportedOperationException("'>' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator LESS_THAN = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type leftType = leftValue.getType();
        Type rightType = rightValue.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'<' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(leftValue.compareTo(rightValue) < 0);
        }
        if (rightType == Type.LIST && leftType == rightValue.getPrimitiveType()) {
            return new TypedObject(rightValue.getList().stream().anyMatch(o -> leftValue.compareTo(new TypedObject(o)) < 0));
        }
        throw new UnsupportedOperationException("'<' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator GREATER_THAN_OR_EQUALS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type leftType = leftValue.getType();
        Type rightType = rightValue.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'>=' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(leftValue.compareTo(rightValue) >= 0);
        }
        if (rightType == Type.LIST && leftType == rightValue.getPrimitiveType()) {
            return new TypedObject(rightValue.getList().stream().anyMatch(o -> leftValue.compareTo(new TypedObject(o)) >= 0));
        }
        throw new UnsupportedOperationException("'>=' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator LESS_THAN_OR_EQUALS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type leftType = leftValue.getType();
        Type rightType = rightValue.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'<=' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(leftValue.compareTo(rightValue) <= 0);
        }
        if (rightType == Type.LIST && leftType == rightValue.getPrimitiveType()) {
            return new TypedObject(rightValue.getList().stream().anyMatch(o -> leftValue.compareTo(new TypedObject(o)) <= 0));
        }
        throw new UnsupportedOperationException("'<=' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator REGEX_LIKE = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type leftType = leftValue.getType();
        Type rightType = rightValue.getType();
        if (leftType != Type.STRING) {
            throw new UnsupportedOperationException("'RLIKE' left operand must have type STRING.");
        }
        if (rightType == Type.STRING) {
            return new TypedObject(Pattern.compile((String) rightValue.getValue()).matcher((String) leftValue.getValue()).matches());
        }
        if (rightType == Type.LIST && leftType == rightValue.getPrimitiveType()) {
            String value = (String) leftValue.getValue();
            return new TypedObject(rightValue.getList().stream().map(o -> Pattern.compile((String) o).matcher(value)).anyMatch(Matcher::matches));
        }
        throw new UnsupportedOperationException("'RLIKE' right operand must have type STRING or have type LIST with primitive type STRING.");
    };

    static BinaryOperator SIZE_IS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        TypedObject size = new TypedObject(leftValue.size());
        if (rightValue.getType() == Type.LIST) {
            return new TypedObject(rightValue.getList().stream().anyMatch(size::equalTo));
        }
        return new TypedObject(rightValue.equalTo(size));
    };

    static BinaryOperator CONTAINS_KEY = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type rightType = rightValue.getType();
        if (rightType == Type.STRING) {
            return new TypedObject(leftValue.containsKey((String) rightValue.getValue()));
        }
        if (rightType == Type.LIST && rightValue.getPrimitiveType() == Type.STRING) {
            return new TypedObject(rightValue.getList().stream().anyMatch(o -> leftValue.containsKey((String) o)));
        }
        throw new UnsupportedOperationException("'CONTAINSKEY' right operand must have type STRING or have type LIST with primitive type STRING.");
    };

    static BinaryOperator CONTAINS_VALUE = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        if (rightValue.getType() == Type.LIST) {
            return new TypedObject(rightValue.getList().stream().map(TypedObject::new).anyMatch(leftValue::containsValue));
        }
        return new TypedObject(leftValue.containsValue(rightValue));
    };

    static BinaryOperator AND = (left, right, record) ->
            new TypedObject((Boolean) left.evaluate(record).forceCast(Type.BOOLEAN).getValue() &&
                            (Boolean) right.evaluate(record).forceCast(Type.BOOLEAN).getValue());

    static BinaryOperator OR = (left, right, record) ->
            new TypedObject((Boolean) left.evaluate(record).forceCast(Type.BOOLEAN).getValue() ||
                            (Boolean) right.evaluate(record).forceCast(Type.BOOLEAN).getValue());

    static BinaryOperator XOR = (left, right, record) ->
            new TypedObject((Boolean) left.evaluate(record).forceCast(Type.BOOLEAN).getValue() ^
                            (Boolean) right.evaluate(record).forceCast(Type.BOOLEAN).getValue());

    static BinaryOperator FILTER = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        if (leftValue.getType() != Type.LIST || rightValue.getType() != Type.LIST || rightValue.getPrimitiveType() != Type.BOOLEAN || leftValue.size() != rightValue.size()) {
            throw new UnsupportedOperationException("'FILTER' operands must be two lists of the same size, and the second list must be booleans.");
        }
        List<Object> list = leftValue.getList();
        List<Object> booleans = rightValue.getList();
        return new TypedObject(IntStream.range(0, list.size()).filter(i -> (Boolean) booleans.get(i)).mapToObj(list::get));
    };

    private static Type getResultType(Type left, Type right) {
        if (!Type.NUMERICS.contains(left) || !Type.NUMERICS.contains(right)) {
            return Type.NULL;
        }
        if (left == Type.DOUBLE || right == Type.DOUBLE) {
            return Type.DOUBLE;
        }
        if (left == Type.FLOAT || right == Type.FLOAT) {
            return Type.FLOAT;
        }
        if (left == Type.LONG || right == Type.LONG) {
            return Type.LONG;
        }
        return Type.INTEGER;
    }
}
