package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Use "Lazy" as a placeholder name because it rolls off the tongue.
 *
 * Lazy expressions are currently used in queries for filters and projections.
 *
 * A filter is simply a lazy expression. We accept a record iff it the expression evaluates to true (with an explicit cast to Boolean), e.g.
 *
 * "filter": { "left": {"field": "bcookie"}, "right": {"value": "123456", "type": "STRING"}, "op": "EQUALS" }
 *
 * A projection is a Map (i.e. Json object) from names to lazy expressions, e.g.
 *
 * "projection": {
 *     "bcookie": {"field": "bcookie"},
 *     "some_constant": {"value": "5.0", "type": "DOUBLE"},
 *     "list_of_fields": {"values": [{"field": "bcookie_age"}, {"field": "bcookie_timestamp"}, {"field": "bcookie_version"}], "type": "STRING"}
 * }
 *
 * Currently, the supported lazy expressions are:
 * - LazyNull
 * - LazyValue
 * - LazyField
 * - LazyUnary
 * - LazyBinary
 * - LazyList
 *
 * LazyMap is not supported at the moment since there's a lot of type-safety to take into consideration.
 *
 * Also, note the "type" field in LazyExpression. This might be the true origin of the "lazy" in lazy expressions. The specified type
 * is not a type-check; rather, when an expression is evaluated, it will force cast to that type (if specified) before returning, so it's like a
 * really late type-check. Also, type must be primitive.
 *
 * Look at the Evaluator class to see how lazy expressions are evaluated.
 */
@Getter
public abstract class LazyExpression implements Configurable, Initializable {
    /** The type of the operation in binary/unary lazy expressions. */
    @Getter
    @AllArgsConstructor
    public enum Operation {
        @SerializedName("+")
        ADD("+", true),
        @SerializedName("-")
        SUB("-", true),
        @SerializedName("*")
        MUL("*", true),
        @SerializedName("/")
        DIV("/", true),
        @SerializedName("==")
        EQUALS("==", true),
        @SerializedName("!=")
        NOT_EQUALS("!=", true),
        @SerializedName(">")
        GREATER_THAN(">", true),
        @SerializedName("<")
        LESS_THAN("<", true),
        @SerializedName(">=")
        GREATER_THAN_OR_EQUALS(">=", true),
        @SerializedName("<=")
        LESS_THAN_OR_EQUALS("<=", true),
        @SerializedName(value = "~=", alternate = { "RLIKE" })
        REGEX_LIKE("RLIKE", false),
        @SerializedName("SIZEIS")
        SIZE_IS("SIZEIS", false),
        @SerializedName("CONTAINSKEY")
        CONTAINS_KEY("CONTAINSKEY", false),
        @SerializedName("CONTAINSVALUE")
        CONTAINS_VALUE("CONTAINSVALUE", false),
        @SerializedName(value = "AND", alternate = { "&&" })
        AND("AND", true),
        @SerializedName(value = "OR", alternate = { "||" })
        OR("OR", true),
        @SerializedName(value = "XOR", alternate = { "^" })
        XOR("XOR", true),
        @SerializedName("FILTER")
        FILTER("FILTER", false),
        @SerializedName(value = "NOT", alternate = { "~" })
        NOT("NOT", false),
        @SerializedName("SIZEOF")
        SIZE_OF("SIZEOF", false);

        public static final Set<Operation> BINARY_OPERATIONS =
                new HashSet<>(asList(ADD, SUB, MUL, DIV,
                                     EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, GREATER_THAN_OR_EQUALS, LESS_THAN_OR_EQUALS,
                                     REGEX_LIKE, SIZE_IS, CONTAINS_KEY, CONTAINS_VALUE,
                                     AND, OR, XOR,
                                     FILTER));
        public static final Set<Operation> UNARY_OPERATIONS =
                new HashSet<>(asList(NOT, SIZE_OF));
        public static final Set<Operation> LOGICALS =
                new HashSet<>(asList(AND, OR, XOR, NOT));
        public static final Set<Operation> RELATIONALS =
                new HashSet<>(asList(EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, GREATER_THAN_OR_EQUALS, LESS_THAN_OR_EQUALS,
                                     REGEX_LIKE, SIZE_IS, CONTAINS_KEY, CONTAINS_VALUE));

        private String name;
        private boolean infix;

        @Override
        public String toString() {
            return name;
        }
    }

    @Expose
    protected Type type;

    /**
     * Gets the name of this expression from its values and operations.
     *
     * @return The name of this expression.
     */
    public abstract String getName();

    /**
     * Constructs an evaluator for this expression and returns it.
     *
     * @return A newly-constructed evaluator for this expression.
     */
    public abstract Evaluator getEvaluator();

    @Override
    public String toString() {
        return "type: " + (type == null ? "null" : type.toString());
    }
}
