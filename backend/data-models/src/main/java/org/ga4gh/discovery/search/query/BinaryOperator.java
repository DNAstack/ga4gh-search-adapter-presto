package org.ga4gh.discovery.search.query;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Arrays;
import java.util.List;
import org.ga4gh.discovery.search.Type;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class BinaryOperator extends Predicate {

    private final Expression leftExpression;
    private final Expression rightExpression;

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
            return Type.STRING;
        } else if (typeStr.equals("boolean")) {
            return Type.STRING;
        } else {
            throw new IllegalArgumentException("Unknown type: " + typeStr);
        }
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
            return FieldReference.parse(node.get(fieldKey).asText());
        } else if (node.has(literalStringKey)) {
            return new LiteralExpression(node.get(literalStringKey).asText(), Type.STRING);
        } else if (node.has(literalBooleanKey)) {
            return new LiteralExpression(node.get(literalBooleanKey).asText(), Type.BOOLEAN);
        } else if (node.has(literalNumberKey)) {
            return new LiteralExpression(node.get(literalNumberKey).asText(), Type.NUMBER);
        } else if (node.has(literalValueKey)) {
            String typeStr = node.has(literalTypeKey) ? node.get(literalTypeKey).asText() : null;
            return new LiteralExpression(node.get(literalValueKey).asText(), toType(typeStr));
        } else {
            throw new IllegalArgumentException();
        }
    }
}
