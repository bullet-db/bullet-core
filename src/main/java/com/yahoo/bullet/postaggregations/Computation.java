/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.expressionparser.CaseInsensitiveStream;
import com.yahoo.bullet.expressionparser.ExpressionLexer;
import com.yahoo.bullet.expressionparser.ExpressionParser;
import com.yahoo.bullet.expressionparser.TypedExpressionVisitor;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.Getter;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;
import static com.yahoo.bullet.common.Utilities.getCasted;

@Getter
public class Computation implements PostStrategy {
    private static final BaseErrorListener BASE_LISTENER = new BaseErrorListener() {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e) {
            throw new RuntimeException(String.format("line %s:%s: %s", line, charPositionInLine + 1, message));
        }
    };

    private static final String EXPRESSION_NAME = "expression";
    public static final BulletError COMPUTATION_REQUIRES_EXPRESSION_ERROR =
            makeError("The COMPUTATION post aggregation needs a non-empty expression", "Please provide a non-empty expression.");
    private static final String NEW_FIELD_NAME = "newName";
    public static final BulletError COMPUTATION_REQUIRES_NEW_FIELD_ERROR =
            makeError("The COMPUTATION post aggregation needs a non-empty new field name", "Please provide a non-empty new field name.");
    public static final String COMPUTATION_PARSING_ERROR_MESSAGE_PREFIX = "The expression of the COMPUTATION post aggregation is invalid: ";
    public static final String COMPUTATION_PARSING_ERROR_RESOLUTION = "Please provide a valid expression.";

    private String expression;
    private String newFieldName;
    // private ParseTree parseTree;

    /**
     * Constructor takes a {@link PostAggregation}.
     *
     * @param aggregation The {@link PostAggregation} for this post aggregation type.
     */
    @SuppressWarnings("unchecked")
    public Computation(PostAggregation aggregation) {
        Map<String, Object> attributes = aggregation.getAttributes();
        if (attributes != null) {
            expression = getCasted(attributes, EXPRESSION_NAME, String.class);
            newFieldName = getCasted(attributes, NEW_FIELD_NAME, String.class);
        }
    }

    @Override
    public Clip execute(Clip clip) {
        ParseTree parseTree = createParsingTree();
        clip.getRecords().forEach(r -> {
                TypedExpressionVisitor visitor = new TypedExpressionVisitor(r);
                try {
                    TypedObject result = visitor.visit(parseTree);
                    switch (result.getType()) {
                        case INTEGER:
                            r.setInteger(newFieldName, (Integer) result.getValue());
                            break;
                        case LONG:
                            r.setLong(newFieldName, (Long) result.getValue());
                            break;
                        case DOUBLE:
                            r.setDouble(newFieldName, (Double) result.getValue());
                            break;
                        case FLOAT:
                            r.setFloat(newFieldName, (Float) result.getValue());
                            break;
                        case BOOLEAN:
                            r.setBoolean(newFieldName, (Boolean) result.getValue());
                            break;
                        case STRING:
                            r.setString(newFieldName, (String) result.getValue());
                            break;
                        default:
                            r.setString(newFieldName, "N/A");
                    }
                } catch (RuntimeException e) {
                    r.setString(newFieldName, "N/A");
                }
            });
        return clip;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (expression == null || expression.isEmpty()) {
            return Optional.of(Collections.singletonList(COMPUTATION_REQUIRES_EXPRESSION_ERROR));
        }
        if (newFieldName == null || newFieldName.isEmpty()) {
            return Optional.of(Collections.singletonList(COMPUTATION_REQUIRES_NEW_FIELD_ERROR));
        }
        try {
            createParsingTree();
        } catch (Exception e) {
            return Optional.of(Collections.singletonList(makeError(COMPUTATION_PARSING_ERROR_MESSAGE_PREFIX + e, COMPUTATION_PARSING_ERROR_RESOLUTION)));
        }
        return Optional.empty();
    }

    private ParseTree createParsingTree() {
        ANTLRInputStream inputStream = new ANTLRInputStream(expression);
        ExpressionLexer lexer = new ExpressionLexer(new CaseInsensitiveStream(inputStream));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        ExpressionParser parser = new ExpressionParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(BASE_LISTENER);
        return parser.expression();
    }
}
