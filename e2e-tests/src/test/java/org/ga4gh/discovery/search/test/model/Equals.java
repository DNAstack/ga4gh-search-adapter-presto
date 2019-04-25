package org.ga4gh.discovery.search.test.model;

import com.fasterxml.jackson.databind.JsonNode;

public class Equals extends BinaryOperator {

    public static final String KEY = "=";

    public Equals(Expression leftExpression, Expression rightExpression) {
        super(leftExpression, rightExpression);
    }

    @Override
    public String getKey() {
        return "=";
    }

    public static Equals fromNode(JsonNode node) {
        return new Equals(expressionFromNode("l", node), expressionFromNode("r", node));
    }

    @Override
    public String toString() {
        return getLeftExpression() + " = " + getRightExpression();
    }
}
