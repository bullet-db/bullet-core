package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

@Getter
public abstract class LazyExpression implements Configurable, Initializable {
    /** The type of the operation in binary/unary lazy expressions. */
    public enum Operation {
        @SerializedName("+")
        ADD,
        @SerializedName("-")
        SUB,
        @SerializedName("*")
        MUL,
        @SerializedName("/")
        DIV,
        @SerializedName("==")
        EQUALS,
        @SerializedName("!=")
        NOT_EQUALS,
        @SerializedName(">")
        GREATER_THAN,
        @SerializedName("<")
        LESS_THAN,
        @SerializedName(">=")
        GREATER_THAN_OR_EQUALS,
        @SerializedName("<=")
        LESS_THAN_OR_EQUALS,
        @SerializedName(value = "~=", alternate = { "RLIKE" })
        REGEX_LIKE,
        @SerializedName("SIZEIS")
        SIZE_IS,
        @SerializedName("CONTAINSKEY")
        CONTAINS_KEY,
        @SerializedName("CONTAINSVALUE")
        CONTAINS_VALUE,
        @SerializedName(value = "&&", alternate = { "AND" })
        AND,
        @SerializedName(value = "||", alternate = { "OR" })
        OR,
        @SerializedName(value = "^^", alternate = { "XOR" })
        XOR,
        @SerializedName("FILTER")
        FILTER,
        @SerializedName(value = "~", alternate = { "NOT" })
        NOT,
        @SerializedName("SIZEOF")
        SIZE_OF;

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
    }

    @Expose
    protected Type type;

    public abstract String getName();

    @Override
    public String toString() {
        return "type: " + (type == null ? "" : type.toString());
    }
}
