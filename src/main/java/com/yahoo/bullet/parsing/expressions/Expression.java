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
 * Expressions are currently used in queries for filters and projections.
 *
 * A filter is simply an expression. We accept a record iff it the expression evaluates to true (with a forced cast to Boolean if necessary), e.g.
 *
 * "filter": { "left": {"field": "bcookie"}, "right": {"value": "123456", "type": "STRING"}, "op": "EQUALS" }
 *
 * A projection is a Map (i.e. Json object) from names to lazy expressions, e.g.
 *
 * "projection": {
 *     "candy": {"field": "candy"},
 *     "price": {"value": "5.0", "type": "DOUBLE"},
 *     "properties": {"values": [{"field": "candy_type"}, {"field": "candy_rarity"}, {"field": "candy_score"}], "type": "STRING"}
 * }
 *
 * Currently, the supported expressions are:
 * - NullExpression
 * - ValueExpression
 * - FieldExpression
 * - UnaryExpression
 * - BinaryExpression
 * - ListExpression
 *
 * MapExpression is not supported at the moment.
 *
 * Also, note the "type" field in Expression. This might be the true origin of the "lazy" in lazy expressions. The specified type
 * is not a type-check; rather, when an expression is evaluated, it will force cast to that type (if specified) before returning, so it is similar to a
 * late type-check. Also, type must be primitive.
 *
 * Look at the Evaluator class to see how expressions are evaluated.
 */
@Getter
public abstract class Expression implements Configurable, Initializable {

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
