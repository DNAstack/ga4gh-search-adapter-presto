package org.ga4gh.discovery.search.source.presto;

import org.ga4gh.discovery.search.query.Equals;
import org.ga4gh.discovery.search.query.Predicate;

public class EqualsTransformer extends PredicateTransformer {

    protected EqualsTransformer(Predicate predicate, QueryContext queryContext) {
        super(predicate, queryContext);
    }

    @Override
    public String toSql() {
        Equals equals = (Equals) super.getPredicate();
        StringBuilder sql = new StringBuilder();
        sql.append(
                ExpressionTransformer.createTransformer(
                                equals.getLeftExpression(), getQueryContext())
                        .toSql());
        sql.append(" = ");
        sql.append(
                ExpressionTransformer.createTransformer(
                                equals.getRightExpression(), getQueryContext())
                        .toSql());
        return sql.toString();
    }
}
