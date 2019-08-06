package org.ga4gh.discovery.search.source.presto;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.sql.ExpressionFormatter.formatExpression;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.query.SearchQueryHelper;

import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Offset;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.Table;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SearchQueryTransformer extends AstVisitor<Void, VisitorContext> {

    private final Metadata metadata;
    private final Query query;
    private final QueryContext queryContext;
    private StringBuilder sql = new StringBuilder();

    @Override
    protected Void visitNode(Node node, VisitorContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Void visitQuery(Query node, VisitorContext context) {
        if (queryContext.hasFromTable(Metadata.PGP_CANADA)) {
            sql.append("WITH " + Metadata.PGP_CANADA + " AS (");
            sql.append(createDemoViewQuerySQL());
            sql.append(")\n");
        }
        checkArgument(!node.getWith().isPresent(), "WITH clause not supported");
        checkArgument(!node.getOrderBy().isPresent(), "ORDER BY clause not supported");
        checkArgument(!node.getLimit().isPresent(), "LIMIT clause not supported");
        process(node.getQueryBody());
        return null;
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, VisitorContext context) {
        checkArgument(node.getFrom().isPresent(), "queries without FROM clause not supported");
        checkArgument(!node.getGroupBy().isPresent(), "GROUP BY clause not supported");
        checkArgument(!node.getHaving().isPresent(), "HAVING clause not supported");
        checkArgument(!node.getOrderBy().isPresent(), "ORDER BY clause not supported");

        process(node.getSelect(), VisitorContext.SELECT);

        sql.append("\nFROM ");
        process(node.getFrom().get(), VisitorContext.FROM);

        if (node.getWhere().isPresent()) {
            sql.append("\nWHERE ");
            sql.append(formatExpression(node.getWhere().get(), Optional.empty()));
        }
        if (node.getOffset().isPresent()) {
            process(node.getOffset().get(), VisitorContext.OFFSET);
        }
        if (node.getLimit().isPresent()) {
            process(node.getLimit().get(), VisitorContext.LIMIT);
        }
        return null;
    }

    @Override
    protected Void visitSelect(Select node, VisitorContext context) {
        if (!node.getSelectItems().isEmpty()) {
            sql.append("SELECT ");
            SelectItem first = node.getSelectItems().get(0);
            process(first);
            for (SelectItem item : node.getSelectItems().subList(1, node.getSelectItems().size())) {
                sql.append(", ");
                process(item);
            }
        }
        return null;
    }

    @Override
    protected Void visitAllColumns(AllColumns node, VisitorContext context) {
        sql.append("* ");
        return null;
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, VisitorContext context) {
        process(node.getExpression());
        if (node.getAlias().isPresent()) {
            sql.append(" AS \"");
            sql.append(node.getAlias().get().getValue());
            sql.append("\"");
        }
        return null;
    }

    @Override
    protected Void visitExpression(Expression node, VisitorContext context) {
        sql.append(formatExpression(node, Optional.empty()));
        return null;
    }

    @Override
    protected Void visitTable(Table node, VisitorContext context) {
        //sql.append(formatName(translateToPresto(node.getName())));
        sql.append(translateToPresto(node.getName()));
        return null;
    }

    private QualifiedName translateToPresto(QualifiedName qualifiedName) {
//        checkArgument(
//                qualifiedName.getParts().size() == 1,
//                "only single part qualified names are supported");
//        String tableName = qualifiedName.getParts().get(0);
        //TODO: HACKS ON HACKS ON HACKS (whatchu need?)
        String tableName = qualifiedName.toString();
        if (Metadata.PGP_CANADA.equals(tableName)) {
            return QualifiedName.of(Metadata.PGP_CANADA);
        } else {
            PrestoTable prestoTable = metadata.getPrestoTable(tableName);
            return prestoTable.getQualifiedName();
        }
    }

    private String formatName(QualifiedName name) {
        return name.getOriginalParts().stream().map(this::formatName).collect(joining("."));
    }

    private String formatName(Identifier name) {
        String delimiter = name.isDelimited() ? "\"" : "";
        return delimiter + name.getValue().replace("\"", "\"\"") + delimiter;
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, VisitorContext context) {
        process(node.getRelation(), VisitorContext.FROM);

        sql.append(" AS ").append(formatExpression(node.getAlias(), Optional.empty()));
        appendAliasColumns(sql, node.getColumnNames());

        return null;
    }

    private static void appendAliasColumns(StringBuilder builder, List<Identifier> columns) {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns =
                    columns.stream()
                            .map(name -> formatExpression(name, Optional.empty()))
                            .collect(Collectors.joining(", "));

            builder.append(" (").append(formattedColumns).append(')');
        }
    }

    protected Void visitJoin(Join node, VisitorContext context) {
        checkArgument(VisitorContext.FROM.equals(context));
        checkArgument(node.getType() == Join.Type.IMPLICIT, "only IMPLICIT joins supported");
        checkArgument(!node.getCriteria().isPresent(), "JOIN criteria not supported yet");
        process(node.getLeft(), VisitorContext.FROM);
        sql.append(",\n");
        process(node.getRight(), VisitorContext.FROM);
        return null;
    }

    @Override
    protected Void visitLimit(Limit node, VisitorContext context) {
        sql.append("\nLIMIT ");
        sql.append(node.getLimit());
        return null;
    }

    @Override
    protected Void visitOffset(Offset node, VisitorContext context) {
        sql.append("\nOFFSET ");
        sql.append(node.getRowCount());
        return null;
    }

    private String createDemoViewQuerySQL() {
        Query demoViewQuery = SearchQueryHelper.demoViewQuery();
        QueryContext demoViewQueryContext = new QueryContext(demoViewQuery, metadata);
        SearchQueryTransformer transformer =
                new SearchQueryTransformer(metadata, demoViewQuery, demoViewQueryContext);
        return transformer.toPrestoSQL();
    }

    public String toPrestoSQL() {
        this.process(query);
        String sql = this.sql.toString();
        this.sql = new StringBuilder();
        return sql;
    }

    public List<Field> validateAndGetFields(ResultSetMetaData rsMetadata) {
        try {
            int numColumns = rsMetadata.getColumnCount();
            checkArgument(
                    numColumns == queryContext.getSelectColumnCount(), "Unexpected column count");
                    //TODO: Another hack --> query context doesn't get columnCount populated correctly when performing
                    // A 'SELECT *' type search.
//                    (numColumns == queryContext.getSelectColumnCount() || queryContext.getSelectColumnCount() == 0), "Unexpected column count");

            for (int i = 0; i < numColumns; i++) {
                int j = i + 1;
                String rsColumn = rsMetadata.getColumnName(j);
                String jdbcRsType = normalizeNestedTypes(rsMetadata.getColumnTypeName(j));

                ResolvedColumn resolvedColumn = queryContext.getSelectColumn(i);
                Optional<String> alias = resolvedColumn.getColumnAlias();
                Field resolvedField = resolvedColumn.getResolvedField();
                Set<String> allowedJdbcTypes = Metadata.jdbcTypesFor(resolvedField.getType());
                if (alias.isPresent()) {
                    checkArgument(
                            alias.get().equals(rsColumn),
                            format("Expected column %d name %s not %s", j, alias.get(), rsColumn));
                } else {
                    checkArgument(
                            resolvedField.getName().equals(rsColumn),
                            format(
                                    "Expected column %d name %s not %s",
                                    j, resolvedField.getName(), rsColumn));
                }
                checkArgument(
                        allowedJdbcTypes.contains(jdbcRsType),
                        format(
                                "Expected column %d type %s must have JDBC type in %s, but was %s",
                                j,
                                resolvedField.getType(),
                                allowedJdbcTypes.toString(),
                                jdbcRsType));
            }

            return queryContext.getSelectFields();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String normalizeNestedTypes(String jdbcType) {
        if (jdbcType.startsWith("varchar(")) {
            return "varchar";
        } else if (jdbcType.startsWith("array(")) {
            return "array";
        } else if (jdbcType.startsWith("row(")) {
            return "row";
        } else {
            return jdbcType;
        }
    }
}
