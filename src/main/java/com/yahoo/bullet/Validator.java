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

/**
 * This class validates instances of {@link BulletConfig}. Use {@link com.yahoo.bullet.Validator.Entry} to define
 * fields and {@link com.yahoo.bullet.Validator.Relationship} to define relationships between them.
 */
@Slf4j
public class Validator {
    private static final Predicate<Object> UNARY_IDENTITY = o -> true;
    private static final BiPredicate<Object, Object> BINARY_IDENTITY = (oA, oB) -> true;

    /**
     * This represents a field in the Validator. It applies a {@link Predicate} to the value of the field and uses a
     * default value (see {@link Entry#defaultTo(Object)} if the predicate fails. It can also apply an arbitrary
     * conversion using {@link Entry#castTo(Function)}. These are all applied when you call
     * {@link Entry#normalize(BulletConfig)} with a {@link BulletConfig} containing a field that matches the Entry.
     */
    public class Entry {
        private String key;
        private Predicate<Object> validation;
        @Getter
        private Object defaultValue;
        private Function<Object, Object> adapter;

        private Entry(String key) {
            this.validation = UNARY_IDENTITY;
            this.key = key;
        }

        /**
         * Add a {@link Predicate} to check for the field represented by the entry. This predicate should take the
         * value of the field and return true to represent a successful validation and false otherwise. You can add
         * more checks by repeatedly calling this method. All your predicates added so far will be ANDed to the latest
         * check.
         *
         * @param validator The non-null validator to use for this Entry.
         * @return This Entry for chaining.
         */
        public Entry checkIf(Predicate<Object> validator) {
            Objects.requireNonNull(validator);
            this.validation.and(validator);
            return this;
        }

        /**
         * Use a default value for the field represented by this Entry. This is used if the validation fails. Note that
         * the {@link Entry#castTo(Function)} will be applied to this if present.
         *
         * @param defaultValue The value to use for the field in the {@link BulletConfig} if validation fails.
         * @return This Entry for chaining.
         */
        public Entry defaultTo(Object defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        /**
         * Apply a cast to the value in the {@link BulletConfig} after validation and defaults are applied. Use this to
         * convert values in the config to their final types if you find yourself type-casting or checking for their
         * types repeatedly.
         *
         * @param adapter The function that takes the field (or the default value) represented and converts it to another.
         * @return This Entry for chaining.
         */
        public Entry castTo(Function<Object, Object> adapter) {
            this.adapter = adapter;
            return this;
        }

        /**
         * Normalizes a {@link BulletConfig} by validating, apply defaults and converting the object represented by the
         * field in this Entry.
         *
         * @param config The config to normalize.
         */
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

    /**
     * This represents a binary relationship between two fields in a {@link BulletConfig}. You should have defined
     * {@link Entry} for these fields before you try to define relationships between them. You can use this apply a
     * {@link BiPredicate} to these fields and provide or use their defined defaults (defined using
     * {@link Entry#defaultTo(Object)}) if the check fails.
     */
    public class Relationship {
        @Getter
        private String keyA;
        @Getter
        private String keyB;
        private String description;
        private BiPredicate<Object, Object> binaryRelation;
        private Object defaultA;
        private Object defaultB;

        private Relationship(String description, String keyA, String keyB) {
            this.description = description;
            this.keyA = keyA;
            this.keyB = keyB;
            // These keys in entries are guaranteed to be present.
            this.defaultA = entries.get(keyA).getDefaultValue();
            this.defaultB = entries.get(keyB).getDefaultValue();
            this.binaryRelation = BINARY_IDENTITY;
        }

        /**
         * Provide the {@link BiPredicate} that acts as the check for this relationship. You can provide more checks
         * and they will be ANDed on the existing checks.
         *
         * @param binaryRelation A check for this relationship.
         * @return This Relationship for chaining.
         */
        public Relationship checkIf(BiPredicate<Object, Object> binaryRelation) {
            this.binaryRelation.and(binaryRelation);
            return this;
        }

        /**
         * Provide custom defaults for this Relationship if you do not want to use the defaults provided in their
         * Entries.
         *
         * @param objectA The default for the first field.
         * @param objectB The default for the second field.
         */
        public void orElseUse(Object objectA, Object objectB) {
            this.defaultA = objectA;
            this.defaultB = objectB;
        }

        /**
         * Normalize the given {@link BulletConfig} for the fields defined by this relationship. This applies the check
         * and uses the defaults (provided using {@link Relationship#orElseUse(Object, Object)} or the Entry defaults
         * for these fields if the check fails.
         *
         * @param config The config to normalize.
         */
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

    /**
     * Creates an instance of the Entry using the name of the field. This field by default will pass the
     * {@link Predicate} unless you add a check using {@link Entry#checkIf(Predicate)}.
     *
     * @param key The name of the field.
     * @return The created {@link Entry}.
     */
    public Entry define(String key) {
        Entry entry = new Entry(key);
        entries.put(key, entry);
        return entry;
    }

    /**
     * Create a relationship with a description for it for the given fields. By default, the relationship will
     * hold true unless you provide a custom check using {@link Relationship#checkIf(BiPredicate)}. By default,
     * if the relationship fails to hold, the defaults defined by the Entries for these fields will be used unless
     * you provide new ones using {@link Relationship#orElseUse(Object, Object)}.
     *
     * @param description A string description of this relationship.
     * @param keyA The first field in the relationship.
     * @param keyB The second field in the relationship.
     * @return The created {@link Relationship}.
     */
    public Relationship relate(String description, String keyA, String keyB) {
        Objects.requireNonNull(entries.get(keyA), "You cannot add a relationship for " + keyA + " before defining it");
        Objects.requireNonNull(entries.get(keyB), "You cannot add a relationship for " + keyB + " before defining it");

        Relationship relation = new Relationship(description, keyA, keyB);
        relations.add(relation);
        return relation;
    }

    /**
     * Validate and normalize the provided {@link BulletConfig} for the defined entries and relationships. Then entries
     * are used to normalize the config first.
     *
     * @param config The config containing fields to normalize.
     */
    public void normalize(BulletConfig config) {
        entries.values().forEach(e -> e.normalize(config));
        relations.stream().forEach(r -> r.normalize(config));
    }

    // Type Adapters

    /**
     * This casts a {@link Number} Object to an {@link Integer}.
     *
     * @param value The value to cast.
     * @return The converted Integer object.
     */
    public static Object asInt(Object value) {
        return ((Number) value).intValue();
    }

    /**
     * This casts a {@link Number} Object to an {@link Float}.
     *
     * @param value The value to cast.
     * @return The converted Float object.
     */
    public static Object asFloat(Object value) {
        return ((Number) value).floatValue();
    }

    /**
     * This casts a {@link Number} Object to an {@link Double}.
     *
     * @param value The value to cast.
     * @return The converted Double object.
     */
    public static Object asDouble(Object value) {
        return ((Number) value).doubleValue();
    }

    // Unary Predicates

    /**
     * Checks to see if the value is null or not.
     *
     * @param value The object to check.
     * @return A boolean denoting if the value was null.
     */
    public static boolean isNotNull(Object value) {
        return value != null;
    }

    /**
     * Checks to see if the value is of the provided type or not.
     *
     * @param value The object to check type for.
     * @param clazz The supposed class of the value.
     * @return A boolean denoting if the value was of the provided class.
     */
    public static boolean isType(Object value, Class clazz) {
        return isNotNull(value) && clazz.isInstance(value);
    }

    /**
     * Checks to see if the value is a {@link Boolean}.
     *
     * @param value The object to check.
     * @return A boolean denoting if the value was a boolean.
     */
    public static boolean isBoolean(Object value) {
        return isType(value, Boolean.class);
    }

    /**
     * Checks to see if the value is a {@link String}.
     *
     * @param value The object to check.
     * @return A boolean denoting if the value was a String.
     */
    public static boolean isString(Object value) {
        return isType(value, String.class);
    }

    /**
     * Checks to see if the value is a {@link List}.
     *
     * @param value The object to check.
     * @return A boolean denoting if the value was a List.
     */
    public static boolean isList(Object value) {
        return isType(value, List.class);
    }

    /**
     * Checks to see if the value is a {@link Map}.
     *
     * @param value The object to check.
     * @return A boolean denoting if the value was a Map.
     */
    public static boolean isMap(Object value) {
        return isType(value, Map.class);
    }

    /**
     * Checks to see if the value is a {@link Number}.
     *
     * @param value The object to check.
     * @return A boolean denoting if the value was a Number.
     */
    public static boolean isNumber(Object value) {
        return isType(value, Number.class);
    }

    /**
     * Checks to see if the value is an integer type. Both {@link Integer} and {@link Long} qualify.
     *
     * @param value The object to check.
     * @return A boolean denoting if the value was an integer.
     */
    public static boolean isInt(Object value) {
        return isType(value, Long.class) || isType(value, Integer.class);
    }

    /**
     * Checks to see if the value is an floating-point. Both {@link Float} and {@link Double} qualify.
     *
     * @param value The object to check.
     * @return A boolean denoting if the value was a floating-point.
     */
    public static boolean isFloat(Object value) {
        return isType(value, Double.class) || isType(value, Float.class);
    }

    /**
     * Checks to see if the value was positive.
     *
     * @param value The object to check.
     * @return A boolean denoting whether the given number value was positive.
     */
    public static boolean isPositive(Object value) {
        return isNumber(value) && ((Number) value).doubleValue() > 0;
    }

    /**
     * Checks to see if the value was a positive integer ({@link Integer} or {@link Long}).
     *
     * @param value The object to check.
     * @return A boolean denoting whether the given number value was a positive integer type.
     */
    public static boolean isPositiveInt(Object value) {
        return isPositive(value) && isInt(value);
    }

    /**
     * Checks to see if the value was a positive integer ({@link Integer} or {@link Long}) and a power of 2.
     *
     * @param value The object to check.
     * @return A boolean denoting whether the given number value was a positive, power of 2 integer type.
     */
    public static boolean isPowerOfTwo(Object value) {
        if (!isPositiveInt(value)) {
            return false;
        }
        int toCheck = ((Number) value).intValue();
        return (toCheck & toCheck - 1) == 0;
    }

    // Binary Predicates.

    /**
     * Checks to see if the first numeric object is greater than or equal to the second numeric object.
     *
     * @param first The first numeric object.
     * @param second The second numeric object.
     * @return A boolean denoting whether the first object is greater or equal to the second.
     */
    public static boolean isGreaterOrEqual(Object first, Object second) {
        return ((Number) first).doubleValue() >= ((Number) second).doubleValue();
    }
}
