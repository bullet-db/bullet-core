/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.result;

import com.yahoo.bullet.record.AvroBulletRecord;
import com.yahoo.bullet.record.BulletRecord;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
@Getter
public class RecordBox {
    private BulletRecord record = new AvroBulletRecord();

    public static RecordBox get() {
        return new RecordBox();
    }

    public final RecordBox addNull(String name) {
        record.setString(name, null);
        return this;
    }

    public final RecordBox add(String name, Object value) {
        if (value instanceof Boolean) {
            record.setBoolean(name, (Boolean) value);
        } else if (value instanceof Integer) {
            record.setInteger(name, (Integer) value);
        } else if (value instanceof Long) {
            record.setLong(name, (Long) value);
        } else if (value instanceof Float) {
            record.setFloat(name, (Float) value);
        } else if (value instanceof Double) {
            record.setDouble(name, (Double) value);
        } else if (value instanceof String) {
            record.setString(name, (String) value) ;
        } else {
            throw new RuntimeException("Unsupported type cannot be added in test code to BulletRecord " + value);
        }
        return this;
    }

    @SafeVarargs
    public final RecordBox addMap(String name, Pair<String, Object>... entries) {
        if (entries != null && entries.length != 0) {
            Object value = findObject(entries);
            if (value instanceof Boolean) {
                record.setBooleanMap(name, asMap(Boolean.class, entries));
            } else if (value instanceof Integer) {
                record.setIntegerMap(name, asMap(Integer.class, entries));
            } else if (value instanceof Long) {
                record.setLongMap(name, asMap(Long.class, entries));
            } else if (value instanceof Float) {
                record.setFloatMap(name, asMap(Float.class, entries));
            } else if (value instanceof Double) {
                record.setDoubleMap(name, asMap(Double.class, entries));
            } else if (value instanceof String) {
                record.setStringMap(name, asMap(String.class, entries));
            } else if (value instanceof Map) {
                record.setMapOfStringMap(name, (Map) asMap(Map.class, entries));
            } else if (value == null) {
                record.setStringMap(name, null);
            } else {
                throw new RuntimeException("Unsupported type cannot be added in test code to BulletRecord " + value);
            }
        }
        return this;
    }

    @SafeVarargs
    public final RecordBox addList(String name, Map<String, Object>... entries) {
        if (entries != null && entries.length != 0) {
            Object value = findObject(entries);
            if (value instanceof Boolean) {
                record.setListOfBooleanMap(name, asList(Boolean.class, entries));
            } else if (value instanceof Integer) {
                record.setListOfIntegerMap(name, asList(Integer.class, entries));
            } else if (value instanceof Long) {
                record.setListOfLongMap(name, asList(Long.class, entries));
            } else if (value instanceof Float) {
                record.setListOfFloatMap(name, asList(Float.class, entries));
            } else if (value instanceof Double) {
                record.setListOfDoubleMap(name, asList(Double.class, entries));
            } else if (value instanceof String) {
                record.setListOfStringMap(name, asList(String.class, entries));
            } else if (value == null) {
                record.setListOfStringMap(name, null);
            } else {
                throw new RuntimeException("Unsupported type cannot be added in test code to BulletRecord " + value);
            }
        }
        return this;
    }

    private Object findObject(Pair<String, Object>... entries) {
        try {
            return Stream.of(entries).filter(Objects::nonNull).filter(e -> e.getValue() != null).findAny().get().getValue();
        } catch (NoSuchElementException nsee) {
            return null;
        }
    }

    private Object findObject(Map<String, Object>... entries) {
        try {
            return Stream.of(entries).filter(Objects::nonNull).filter(e -> !e.isEmpty()).findAny()
                         .get().entrySet().stream().findAny().get().getValue();
        } catch (NoSuchElementException nsee) {
            return null;
        }
    }

    private <T> Map<String, T> asMap(Class<T> clazz, Map.Entry<String, Object>... entries) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(entries);
        Map<String, T> newMap = new LinkedHashMap<>(entries.length);
        for (Map.Entry<String, Object> entry : entries) {
            Object object = entry.getValue();
            if (object != null && !clazz.isInstance(object)) {
                throw new RuntimeException("Object " + object + " is not an instance of class " + clazz.getName());
            }
            newMap.put(entry.getKey(), (T) entry.getValue());
        }
        return newMap;
    }

    private <T> List<Map<String, T>> asList(Class<T> clazz, Map<String, Object>... entries) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(entries);
        List<Map<String, T>> newList = new ArrayList<>(entries.length);
        for (Map<String, Object> entry : entries) {
            Set<Map.Entry<String, Object>> data = entry.entrySet();
            Map<String, T> newMap = asMap(clazz, data.toArray(new Map.Entry[data.size()]));
            newList.add(newMap);
        }
        return newList;
    }

}
