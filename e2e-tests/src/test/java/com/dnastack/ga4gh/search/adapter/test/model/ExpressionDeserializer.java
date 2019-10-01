package com.dnastack.ga4gh.search.adapter.test.model;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.ComparisonExpression.Operator;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.StringLiteral;

public class ExpressionDeserializer extends JsonDeserializer<Expression> {

    public static final String EQUALS = "=";
    public static final String AND = "and";
    public static final String LIKE = "like";

    @Override
    public Expression deserialize(JsonParser jp, DeserializationContext dc)
            throws IOException, JsonProcessingException {
        ObjectCodec codec = jp.getCodec();
        return expressionFromNode(codec.readTree(jp));
    }

    public static Expression expressionFromNode(JsonNode node) {
        checkArgument(node.has("p"), "Predicate has to have the predicate type \"p\"");
        String predicateType = node.get("p").asText();
        if (EQUALS.equals(predicateType)) {
            return parseEquals(node);
        } else if (AND.equals(predicateType)) {
            return parseAnd(node);
        } else if (LIKE.equals(predicateType)) {
            return parseLike(node);
        } else {
            throw new IllegalArgumentException("Unknown predicate type: " + predicateType);
        }
    }

    public static Expression parseLike(JsonNode node) {
        throw new UnsupportedOperationException("like not yet supported");
    }

    public static Expression parseEquals(JsonNode node) {
        return new ComparisonExpression(
                Operator.EQUAL, expressionFromNode("l", node), expressionFromNode("r", node));
    }

    protected static Expression expressionFromNode(String side, JsonNode node) {
        String fieldKey = side + "field";
        String literalValueKey = side + "value";
        String literalStringKey = side + "string";
        String literalBooleanKey = side + "boolean";
        String literalNumberKey = side + "number";
        String literalTypeKey = side + "type";
        List<String> requiredKeys =
                Arrays.asList(
                        fieldKey,
                        literalValueKey,
                        literalStringKey,
                        literalBooleanKey,
                        literalNumberKey);
        checkArgument(
                hasExactlyOneOf(node, requiredKeys),
                "Binary operator needs to have exactly one of attributes "
                        + requiredKeys
                        + " present");
        if (node.has(fieldKey)) {
            return parseField(node.get(fieldKey).asText());
        } else if (node.has(literalStringKey)) {
            return new StringLiteral(node.get(literalStringKey).asText());
        } else if (node.has(literalBooleanKey)) {
            return new BooleanLiteral(node.get(literalBooleanKey).asText());
        } else if (node.has(literalNumberKey)) {
            return new DecimalLiteral(node.get(literalNumberKey).asText());
        } else if (node.has(literalValueKey)) {
            String typeStr = node.has(literalTypeKey) ? node.get(literalTypeKey).asText() : null;
            Type type = toType(typeStr);
            if (type == Type.STRING) {
                return new StringLiteral(node.get(literalValueKey).asText());
            } else if (type == Type.NUMBER) {
                return new DecimalLiteral(node.get(literalValueKey).asText());
            } else if (type == Type.BOOLEAN) {
                return new BooleanLiteral(node.get(literalValueKey).asText());
            } else {
                throw new IllegalArgumentException("Unknown type: " + typeStr);
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    static Expression parseField(String reference) {
        String[] fieldTuple = reference.split("\\.");
        checkArgument(fieldTuple.length < 3, "Wrong field reference format: " + reference);
        Identifier first = new Identifier(fieldTuple[0]);
        if (fieldTuple.length == 1) {
            return first;
        } else {
            return new DereferenceExpression(first, new Identifier(fieldTuple[1]));
        }
    }

    private static boolean hasExactlyOneOf(JsonNode node, List<String> keys) {
        int count = 0;
        for (String key : keys) {
            if (node.has(key)) {
                count++;
            }
        }
        return count == 1;
    }

    private static Type toType(String typeStr) {
        if (typeStr == null) {
            return Type.STRING;
        } else if (typeStr.equals("string")) {
            return Type.STRING;
        } else if (typeStr.equals("number")) {
            return Type.NUMBER;
        } else if (typeStr.equals("boolean")) {
            return Type.BOOLEAN;
        } else {
            throw new IllegalArgumentException("Unknown type: " + typeStr);
        }
    }

    public static Expression parseAnd(JsonNode node) {
        checkArgument(AND.equals(node.get("p").asText()));
        ImmutableList.Builder<Expression> expressionListBuilder = ImmutableList.builder();
        for (final JsonNode predicateNode : node.get("predicates")) {
            expressionListBuilder.add(expressionFromNode(predicateNode));
        }
        return createAnd(expressionListBuilder.build());
    }

    private static Expression createAnd(List<Expression> expressions) {
        checkArgument(!expressions.isEmpty(), "AND predicate has to have expressions");
        if (expressions.size() == 1) {
            return expressions.get(0);
        }
        Expression last = expressions.get(expressions.size() - 1);
        List<Expression> rest = expressions.subList(0, expressions.size() - 1);
        return new LogicalBinaryExpression(
                io.prestosql.sql.tree.LogicalBinaryExpression.Operator.AND, createAnd(rest), last);
    }
}
