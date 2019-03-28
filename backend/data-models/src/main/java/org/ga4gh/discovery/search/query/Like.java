package org.ga4gh.discovery.search.query;

import com.fasterxml.jackson.databind.JsonNode;

public class Like extends BinaryOperator {

    public static final String KEY = "like";

    protected Like(Expression leftExpression, Expression rightExpression) {
        super(leftExpression, rightExpression);
    }

    @Override
    public String getKey() {
        return "like";
    }

    public static Like fromNode(JsonNode node) {
        return new Like(expressionFromNode("l", node), expressionFromNode("r", node));
    }

    @Override
    public String toString() {
        return getLeftExpression() + " LIKE " + getRightExpression();
    }
}
