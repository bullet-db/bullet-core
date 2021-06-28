/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.common.Utilities.isNull;

public class NAryOperations {
    @FunctionalInterface
    public interface NAryOperator extends Serializable {
        TypedObject apply(List<Evaluator> evaluator, BulletRecord record);
    }

    static final Map<Operation, NAryOperator> N_ARY_OPERATORS = new EnumMap<>(Operation.class);

    static {
        N_ARY_OPERATORS.put(Operation.AND, NAryOperations::allMatch);
        N_ARY_OPERATORS.put(Operation.OR, NAryOperations::anyMatch);
        N_ARY_OPERATORS.put(Operation.IF, NAryOperations::ternary);
        N_ARY_OPERATORS.put(Operation.BETWEEN, NAryOperations::between);
        N_ARY_OPERATORS.put(Operation.NOT_BETWEEN, NAryOperations::notBetween);
        N_ARY_OPERATORS.put(Operation.SUBSTRING, NAryOperations::substring);
        N_ARY_OPERATORS.put(Operation.UNIX_TIMESTAMP, NAryOperations::unixTimestamp);
    }

    static TypedObject allMatch(List<Evaluator> evaluators, BulletRecord record) {
        boolean containsNull = false;
        for (Evaluator evaluator : evaluators) {
            TypedObject value = evaluator.evaluate(record);
            if (isNull(value)) {
                containsNull = true;
            } else if (!((Boolean) value.forceCast(Type.BOOLEAN).getValue())) {
                return TypedObject.FALSE;
            }
        }
        return !containsNull ? TypedObject.TRUE : TypedObject.NULL;
    }

    static TypedObject anyMatch(List<Evaluator> evaluators, BulletRecord record) {
        boolean containsNull = false;
        for (Evaluator evaluator : evaluators) {
            TypedObject value = evaluator.evaluate(record);
            if (isNull(value)) {
                containsNull = true;
            } else if ((Boolean) value.forceCast(Type.BOOLEAN).getValue()) {
                return TypedObject.TRUE;
            }
        }
        return !containsNull ? TypedObject.FALSE : TypedObject.NULL;
    }

    static TypedObject ternary(List<Evaluator> evaluators, BulletRecord record) {
        TypedObject condition = evaluators.get(0).evaluate(record);
        return !isNull(condition) && (Boolean) condition.getValue() ? evaluators.get(1).evaluate(record) :
                                                                      evaluators.get(2).evaluate(record);
    }

    static TypedObject between(List<Evaluator> evaluators, BulletRecord record) {
        TypedObject valueArg = evaluators.get(0).evaluate(record);
        if (isNull(valueArg)) {
            return TypedObject.NULL;
        }
        if (Type.isNumeric(valueArg.getType())) {
            double value = ((Number) valueArg.getValue()).doubleValue();
            TypedObject lowerArg = evaluators.get(1).evaluate(record);
            TypedObject upperArg = evaluators.get(2).evaluate(record);
            Number lower = (Number) lowerArg.getValue();
            Number upper = (Number) upperArg.getValue();
            if (isNull(lowerArg) && isNull(upperArg)) {
                return TypedObject.NULL;
            } else if (isNull(lowerArg)) {
                return upper.doubleValue() < value ? TypedObject.FALSE : TypedObject.NULL;
            } else if (isNull(upperArg)) {
                return value < lower.doubleValue() ? TypedObject.FALSE : TypedObject.NULL;
            }
            return TypedObject.valueOf(lower.doubleValue() <= value && value <= upper.doubleValue());
        } else {
            String value = (String) valueArg.getValue();
            TypedObject lowerArg = evaluators.get(1).evaluate(record);
            TypedObject upperArg = evaluators.get(2).evaluate(record);
            String lower = (String) lowerArg.getValue();
            String upper = (String) upperArg.getValue();
            if (isNull(lowerArg) && isNull(upperArg)) {
                return TypedObject.NULL;
            } else if (lowerArg.isNull()) {
                return upper.compareTo(value) < 0 ? TypedObject.FALSE : TypedObject.NULL;
            } else if (upperArg.isNull()) {
                return value.compareTo(lower) < 0 ? TypedObject.FALSE : TypedObject.NULL;
            }
            return TypedObject.valueOf(lower.compareTo(value) <= 0 && value.compareTo(upper) <= 0);
        }
    }

    static TypedObject notBetween(List<Evaluator> evaluators, BulletRecord record) {
        TypedObject result = between(evaluators, record);
        if (result.isNull()) {
            return TypedObject.NULL;
        }
        return (Boolean) result.getValue() ? TypedObject.FALSE : TypedObject.TRUE;
    }

    static TypedObject substring(List<Evaluator> evaluators, BulletRecord record) {
        TypedObject stringArg = evaluators.get(0).evaluate(record);
        if (isNull(stringArg)) {
            return TypedObject.NULL;
        }
        TypedObject startArg = evaluators.get(1).evaluate(record);
        if (isNull(startArg)) {
            return TypedObject.NULL;
        }
        TypedObject lengthArg = null;
        if (evaluators.size() > 2) {
            lengthArg = evaluators.get(2).evaluate(record);
            if (isNull(lengthArg)) {
                return TypedObject.NULL;
            }
        }
        String string = (String) stringArg.getValue();
        if (string.isEmpty()) {
            return TypedObject.valueOf("");
        }
        int start = ((Number) startArg.getValue()).intValue();
        if (start == 0 || Math.abs(start) > string.length()) {
            return TypedObject.valueOf("");
        }
        // Change start to 0-index
        if (start > 0) {
            start--;
        } else {
            start += string.length();
        }
        if (evaluators.size() == 2) {
            return TypedObject.valueOf(string.substring(start));
        }
        int length = ((Number) lengthArg.getValue()).intValue();
        if (length <= 0) {
            return TypedObject.valueOf("");
        }
        int end = Math.min(start + length, string.length());
        return TypedObject.valueOf(string.substring(start, end));
    }

    static TypedObject unixTimestamp(List<Evaluator> evaluators, BulletRecord record) {
        if (evaluators.size() == 1) {
            TypedObject dateArg = evaluators.get(0).evaluate(record);
            if (isNull(dateArg)) {
                return TypedObject.NULL;
            }
            Timestamp timestamp = Timestamp.valueOf((String) dateArg.getValue());
            return TypedObject.valueOf(timestamp.toLocalDateTime().toEpochSecond(ZoneOffset.UTC));
        } else if (evaluators.size() == 2) {
            TypedObject dateArg = evaluators.get(0).evaluate(record);
            if (isNull(dateArg)) {
                return TypedObject.NULL;
            }
            TypedObject patternArg = evaluators.get(1).evaluate(record);
            if (isNull(patternArg)) {
                return TypedObject.NULL;
            }
            // First argument can be a number
            String date = Type.isNumeric(dateArg.getType()) ? Long.toString(((Number) dateArg.getValue()).longValue()) : (String) dateArg.getValue();
            String pattern = (String) patternArg.getValue();
            LocalDateTime localDateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(pattern));
            return TypedObject.valueOf(localDateTime.toEpochSecond(ZoneOffset.UTC));
        }
        return TypedObject.valueOf(System.currentTimeMillis() / 1000);
    }
}
