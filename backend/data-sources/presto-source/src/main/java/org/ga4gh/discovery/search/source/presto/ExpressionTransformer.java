package org.ga4gh.discovery.search.source.presto;

import org.ga4gh.discovery.search.query.Expression;
import org.ga4gh.discovery.search.query.FieldReference;
import org.ga4gh.discovery.search.query.LiteralExpression;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class ExpressionTransformer {

    private final Expression expression;
    private final QueryContext queryContext;

    public abstract String toSql();

    public static ExpressionTransformer createTransformer(
            Expression expression, QueryContext queryContext) {
        if (expression instanceof FieldReference) {
            return new FieldReferenceTransformer(expression, queryContext);
        } else if (expression instanceof LiteralExpression) {
            return new LiteralTransformer(expression, queryContext);
        } else {
            throw new IllegalArgumentException("Unknown expression type: " + expression);
        }
    }
}
