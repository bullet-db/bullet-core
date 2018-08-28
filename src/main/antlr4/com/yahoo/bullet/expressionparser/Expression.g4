/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

grammar Expression;

expression
    : expression op=('*' | '/') expression      # mulDivExpression
    | expression op=('+' | '-') expression      # addSubExpression
    | value                                     # valueExpression
    | field                                     # fieldExpression
    | '(' expression ')'                        # parensExpression
    | SIZEOF '(' field ')'                      # sizeofExpression
    | CAST '(' expression ',' type ')'          # castExpression
    ;

type
    : INTEGER_TYPE | LONG_TYPE | FLOAT_TYPE | DOUBLE_TYPE | BOOLEAN_TYPE | STRING_TYPE
    ;

field
    : FIELD '(' referenceExpression ')'
    ;

referenceExpression
    : identifier                                                                          # columnReference
    | base=identifier '.' fieldName=identifier                                            # dereference
    ;

value
    : number                                                                              # numericLiteral
    | bool                                                                                # booleanLiteral
    | string                                                                              # stringLiteral
    | operator=('+' | '-') number                                                         # arithmeticUnary
    ;

identifier
    : IDENTIFIER
    | '"' IDENTIFIER '"'
    | '\'' IDENTIFIER '\''
    ;

number
    : DECIMAL_VALUE                                                                       # decimalLiteral
    | DOUBLE_VALUE                                                                        # doubleLiteral
    | INTEGER_VALUE                                                                       # integerLiteral
    ;

bool
    : TRUE | FALSE
    ;

string
    :  STRING
    ;

FIELD : 'FIELD' ;
SIZEOF : 'SIZEOF' ;
CAST : 'CAST' ;
INTEGER_TYPE : 'INTEGER' ;
LONG_TYPE : 'LONG' ;
FLOAT_TYPE : 'FLOAT' ;
DOUBLE_TYPE : 'DOUBLE' ;
BOOLEAN_TYPE : 'BOOLEAN' ;
STRING_TYPE : 'STRING' ;
TRUE : 'TRUE' ;
FALSE : 'FALSE' ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    | '"' ( ~'"' | '""' )* '"'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

WS : [ \t\r\n]+ -> skip ;
