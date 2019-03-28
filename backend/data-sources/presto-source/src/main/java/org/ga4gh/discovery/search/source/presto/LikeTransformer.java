package org.ga4gh.discovery.search.source.presto;

import org.ga4gh.discovery.search.query.Equals;
import org.ga4gh.discovery.search.query.Predicate;

public class LikeTransformer extends PredicateTransformer {

    public LikeTransformer(Predicate predicate, QueryContext queryContext) {
        super(predicate, queryContext);
        // TODO Auto-generated constructor stub
    }

    @Override
    public String toSql() {
        Equals equals = (Equals) super.getPredicate();
        StringBuilder sql = new StringBuilder();
        sql.append(
                ExpressionTransformer.createTransformer(
                                equals.getLeftExpression(), getQueryContext())
                        .toSql());
        sql.append(" LIKE ");
        sql.append(
                ExpressionTransformer.createTransformer(
                                equals.getRightExpression(), getQueryContext())
                        .toSql());
        return sql.toString();
    }
}
