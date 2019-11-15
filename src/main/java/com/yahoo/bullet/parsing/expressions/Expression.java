package com.yahoo.bullet.parsing.expressions;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

/**
 * Expressions are currently used in queries for filters and projections.
 *
 * A filter is simply an expression. We accept a record iff the expression evaluates to true (with a forced cast to Boolean if necessary), e.g.
 *
 * "filter": {"left": {"field": "id"}, "right": {"value": "123456", "type": "STRING"}, "op": "EQUALS"}
 *
 * A projection is a Map (i.e. Json object) from names to expressions, e.g.
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
 * Note, the "type" field in Expression is not a type-check. When an expression is evaluated, it will force cast to that
 * type (if specified) before returning. Also, type must be primitive.
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
