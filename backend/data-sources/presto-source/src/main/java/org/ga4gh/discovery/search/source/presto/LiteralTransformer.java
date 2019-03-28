package org.ga4gh.discovery.search.source.presto;

import org.ga4gh.discovery.search.query.Expression;
import org.ga4gh.discovery.search.query.LiteralExpression;
import lombok.Getter;

@Getter
public class LiteralTransformer extends ExpressionTransformer {

    protected LiteralTransformer(Expression expression, QueryContext queryContext) {
        super(expression, queryContext);
    }

    @Override
    public String toSql() {
        LiteralExpression literal = (LiteralExpression) getExpression();
        switch (literal.getType()) {
            case STRING:
                return "'" + literal.getValue() + "'";
            case BOOLEAN:
            case NUMBER:
                return literal.getValue();
            case DATE:
            case JSON:
            case STRING_ARRAY:
            default:
                throw new IllegalArgumentException("Type not yet supported: " + literal.getType());
        }
    }
}
