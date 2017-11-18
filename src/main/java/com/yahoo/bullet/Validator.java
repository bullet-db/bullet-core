package com.yahoo.bullet;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public class Validator {
    public static final Predicate<Object> UNARY_IDENTITY = o -> true;
    public static final BiPredicate<Object, Object> BINARY_IDENTITY = (oA, oB) -> true;

    public class Entry {
        private String key;
        private Predicate<Object> validation;
        @Getter
        private Object defaultValue;
        private Function<Object, Object> adapter;

        public Entry(String key) {
            this.validation = UNARY_IDENTITY;
            this.key = key;
        }

        public Entry checkIf(Predicate<Object> validator) {
            this.validation.and(validator);
            return this;
        }

        public Entry defaultTo(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Entry castTo(Function<Object, Object> adapter) {
            this.adapter = adapter;
            return this;
        }

        public void normalize(BulletConfig config) {
            Object value = config.get(key);
            boolean isValid = validation.test(value);
            if (!isValid) {
                log.warn("Key: {} had an invalid value: {}. Using default: {}", key, value, defaultValue);
                value = defaultValue;
            }
            if (adapter != null) {
                value = adapter.apply(value);
                log.info("Changed the type for {}: {}", key, value);
            }
            config.set(key, value);
        }
    }

    public class Relationship {
        @Getter
        private String keyA;
        @Getter
        private String keyB;
        private String description;
        private BiPredicate<Object, Object> binaryRelation;
        private Object defaultA;
        private Object defaultB;

        public Relationship(String description, String keyA, String keyB) {
            this.description = description;
            this.keyA = keyA;
            this.keyB = keyB;
            // These are guaranteed to be present.
            this.defaultA = entries.get(keyA).getDefaultValue();
            this.defaultB = entries.get(keyB).getDefaultValue();
            this.binaryRelation = BINARY_IDENTITY;
        }

        public Relationship checkIf(BiPredicate<Object, Object> binaryRelation) {
            this.binaryRelation.and(binaryRelation);
            return this;
        }

        public void orElseUse(Object objectA, Object objectB) {
            this.defaultA = objectA;
            this.defaultB = objectB;
        }

        public void normalize(BulletConfig config) {
            Object objectA = config.get(keyA);
            Object objectB = config.get(keyB);
            boolean result = binaryRelation.test(objectA, objectB);
            if (!result) {
                log.warn("{}: {} and {}: {} do not satisfy: {}. Using their defaults", keyA, objectA, keyB, objectB, description);
                log.warn("Using default {} for {}", defaultA, keyA);
                log.warn("Using default {} for {}", defaultB, keyB);
                config.set(keyA, defaultA);
                config.set(keyB, defaultB);
            }
        }
    }

    private Map<String, Entry> entries = new HashMap<>();
    private List<Relationship> relations = new ArrayList<>();

    public Entry define(String key) {
        Entry entry = new Entry(key);
        entries.put(key, entry);
        return entry;
    }

    public Relationship relatesTo(String description, String keyA, String keyB) {
        Objects.requireNonNull(entries.get(keyA), "You cannot add a relationship for " + keyA + " before defining it");
        Objects.requireNonNull(entries.get(keyB), "You cannot add a relationship for " + keyB + " before defining it");

        Relationship relation = new Relationship(description, keyA, keyB);
        relations.add(relation);
        return relation;
    }

    public void normalize(BulletConfig config) {
        entries.values().forEach(e -> e.normalize(config));
        relations.stream().forEach(r -> r.normalize(config));
    }

    // Type Adapters

    public static Object asInt(Object value) {
        return ((Number) value).intValue();
    }

    public static Object asFloat(Object value) {
        return ((Number) value).floatValue();
    }

    // Validators

    public static boolean isNotNull(Object value) {
        return value != null;
    }

    public static boolean isType(Object value, Class clazz) {
        return isNotNull(value) && clazz.isInstance(value);
    }

    public static boolean isBoolean(Object value) {
        return isType(value, Boolean.class);
    }

    public static boolean isString(Object value) {
        return isType(value, String.class);
    }

    public static boolean isList(Object value) {
        return isType(value, List.class);
    }

    public static boolean isMap(Object value) {
        return isType(value, Map.class);
    }

    public static boolean isNumber(Object value) {
        return isType(value, Number.class);
    }

    public static boolean isInt(Object value) {
        return isType(value, Long.class) || isType(value, Integer.class);
    }

    public static boolean isFloat(Object value) {
        return isType(value, Double.class) || isType(value, Float.class);
    }

    public static boolean isPositive(Object value) {
        return isNumber(value) && ((Number) value).doubleValue() > 0;
    }

    public static boolean isPositiveInt(Object value) {
        return isPositive(value) && isInt(value);
    }

    public static boolean isPowerOfTwo(Object value) {
        if (!isPositiveInt(value)) {
            return false;
        }
        int toCheck = ((Number) value).intValue();
        return (toCheck & toCheck - 1) == 0;
    }

    // Binary Validators

    public static boolean isGreaterOrEqual(Object first, Object second) {
        return ((Number) first).doubleValue() >= ((Number) second).doubleValue();
    }
}
