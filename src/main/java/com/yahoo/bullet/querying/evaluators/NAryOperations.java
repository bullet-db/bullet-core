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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

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
            if (value.isNull()) {
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
            if (value.isNull()) {
                containsNull = true;
            } else if ((Boolean) value.forceCast(Type.BOOLEAN).getValue()) {
                return TypedObject.TRUE;
            }
        }
        return !containsNull ? TypedObject.FALSE : TypedObject.NULL;
    }

    static TypedObject ternary(List<Evaluator> evaluators, BulletRecord record) {
        TypedObject condition = evaluators.get(0).evaluate(record);
        return !condition.isNull() && (Boolean) condition.getValue() ? evaluators.get(1).evaluate(record) :
                                                                       evaluators.get(2).evaluate(record);
    }

    static TypedObject between(List<Evaluator> evaluators, BulletRecord record) {
        TypedObject valueArg = evaluators.get(0).evaluate(record);
        if (valueArg.isNull()) {
            return TypedObject.NULL;
        }
        double value = ((Number) valueArg.getValue()).doubleValue();
        TypedObject lowerArg = evaluators.get(1).evaluate(record);
        TypedObject upperArg = evaluators.get(2).evaluate(record);
        Number lower = (Number) lowerArg.getValue();
        Number upper = (Number) upperArg.getValue();
        if (lowerArg.isNull() && upperArg.isNull()) {
            return TypedObject.NULL;
        } else if (lowerArg.isNull()) {
            return upper.doubleValue() < value ? TypedObject.FALSE : TypedObject.NULL;
        } else if (upperArg.isNull()) {
            return value < lower.doubleValue() ? TypedObject.FALSE : TypedObject.NULL;
        }
        return TypedObject.valueOf(lower.doubleValue() <= value && value <= upper.doubleValue());
    }

    static TypedObject notBetween(List<Evaluator> evaluators, BulletRecord record) {
        TypedObject valueArg = evaluators.get(0).evaluate(record);
        if (valueArg.isNull()) {
            return TypedObject.NULL;
        }
        double value = ((Number) valueArg.getValue()).doubleValue();
        TypedObject lowerArg = evaluators.get(1).evaluate(record);
        TypedObject upperArg = evaluators.get(2).evaluate(record);
        Number lower = (Number) lowerArg.getValue();
        Number upper = (Number) upperArg.getValue();
        if (lowerArg.isNull() && upperArg.isNull()) {
            return TypedObject.NULL;
        } else if (lowerArg.isNull()) {
            return upper.doubleValue() < value ? TypedObject.TRUE : TypedObject.NULL;
        } else if (upperArg.isNull()) {
            return value < lower.doubleValue() ? TypedObject.TRUE : TypedObject.NULL;
        }
        return TypedObject.valueOf(value < lower.doubleValue() || value > upper.doubleValue());
    }

    static TypedObject substring(List<Evaluator> evaluators, BulletRecord record) {
        TypedObject stringArg = evaluators.get(0).evaluate(record);
        if (stringArg.isNull()) {
            return TypedObject.NULL;
        }
        TypedObject startArg = evaluators.get(1).evaluate(record);
        if (startArg.isNull()) {
            return TypedObject.NULL;
        }
        TypedObject lengthArg = null;
        if (evaluators.size() > 2) {
            lengthArg = evaluators.get(2).evaluate(record);
            if (lengthArg.isNull()) {
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
            if (dateArg.isNull()) {
                return TypedObject.NULL;
            }
            Timestamp dateTime = Timestamp.valueOf((String) dateArg.getValue());
            return TypedObject.valueOf(dateTime.getTime() / 1000);
        } else if (evaluators.size() == 2) {
            TypedObject dateArg = evaluators.get(0).evaluate(record);
            if (dateArg.isNull()) {
                return TypedObject.NULL;
            }
            TypedObject patternArg = evaluators.get(1).evaluate(record);
            if (patternArg.isNull()) {
                return TypedObject.NULL;
            }
            // First argument can be a number
            String date = Type.isNumeric(dateArg.getType()) ? Long.toString(((Number) dateArg.getValue()).longValue()) : (String) dateArg.getValue();
            String pattern = (String) patternArg.getValue();
            LocalDateTime localDateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(pattern));
            return TypedObject.valueOf(localDateTime.atZone(ZoneId.systemDefault()).toEpochSecond());
        }
        return TypedObject.valueOf(System.currentTimeMillis() / 1000);
    }
}
