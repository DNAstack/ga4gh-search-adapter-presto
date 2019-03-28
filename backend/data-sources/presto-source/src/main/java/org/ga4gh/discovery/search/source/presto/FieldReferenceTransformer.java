package org.ga4gh.discovery.search.source.presto;

import org.ga4gh.discovery.search.query.Expression;
import org.ga4gh.discovery.search.query.FieldReference;

public class FieldReferenceTransformer extends ExpressionTransformer {

    protected FieldReferenceTransformer(Expression expression, QueryContext queryContext) {
        super(expression, queryContext);
    }

    @Override
    public String toSql() {
        FieldReference fieldRef = (FieldReference) getExpression();
        StringBuilder s = new StringBuilder();
        fieldRef.getTableReference()
                .ifPresent(
                        tableRef -> {
                            s.append("\"");
                            s.append(tableRef);
                            s.append("\".");
                        });
        s.append("\"");
        s.append(fieldRef.getFieldName());
        s.append("\"");
        return s.toString();
    }
}
