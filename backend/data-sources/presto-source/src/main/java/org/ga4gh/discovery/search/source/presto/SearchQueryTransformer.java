package org.ga4gh.discovery.search.source.presto;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Type;
import org.ga4gh.discovery.search.query.And;
import org.ga4gh.discovery.search.query.Predicate;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.query.SearchQueryField;
import org.ga4gh.discovery.search.query.SearchQueryTable;
import com.google.common.base.Joiner;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SearchQueryTransformer {

    private final Metadata metadata;
    private final SearchQuery query;
    private final QueryContext queryContext;

    private PredicateTransformer createTransformer(Predicate predicate) {
        return PredicateTransformer.createTransformer(predicate, queryContext);
    }

    private String createDemoViewQuerySQL() {
        SearchQuery demoViewQuery = SearchQueryHelper.demoViewQuery();
        QueryContext demoViewQueryContext = new QueryContext(demoViewQuery, metadata);
        SearchQueryTransformer transformer =
                new SearchQueryTransformer(metadata, demoViewQuery, demoViewQueryContext);
        return transformer.toPrestoSQL();
    }

    private OptionalLong limitAndOffset() {
        if (query.getLimit().isPresent()) {
            return query.getOffset().isPresent()
                    ? OptionalLong.of(query.getOffset().getAsLong() + query.getLimit().getAsLong())
                    : query.getLimit();
        } else {
            return OptionalLong.empty();
        }
    }

    public String toPrestoSQL() {
        StringBuilder sql = new StringBuilder();

        if (queryContext.hasFromTable(Metadata.DEMO_VIEW)) {
            sql.append("WITH demo_view AS (");
            sql.append(createDemoViewQuerySQL());
            sql.append(")\n");
        }

        sql.append("SELECT ");
        sql.append(generateSelect());

        sql.append("\nFROM ");
        sql.append(generateFrom());

        simplify(query.getWhere())
                .ifPresent(
                        predicate -> {
                            sql.append("\nWHERE ");
                            sql.append(createTransformer(predicate).toSql());
                        });

        limitAndOffset()
                .ifPresent(
                        limit -> {
                            sql.append("\nLIMIT ");
                            sql.append(Long.toString(limit));
                        });

        return sql.toString();
    }

    private Optional<Predicate> simplify(Optional<Predicate> where) {
        if (where.isPresent()) {
            if (where.get().getKey().equals(And.KEY)) {
                And and = (And) where.get();
                if (and.getPredicates().isEmpty()) {
                    return Optional.empty();
                } else if (and.getPredicates().size() == 1) {
                    return Optional.of(and.getPredicates().get(0));
                } else {
                    return where;
                }
            } else {
                return where;
            }
        } else {
            return Optional.empty();
        }
    }

    private String generateSelect() {
        if (query.getSelect().isEmpty()) {
            return "*";
        }
        return Joiner.on(", ")
                .join(query.getSelect().stream().map(f -> toSQL(f)).collect(toList()));
    }

    public List<Field> validateAndGetFields(ResultSetMetaData rsMetadata) {
        try {
            int numColumns = rsMetadata.getColumnCount();
            checkArgument(
                    numColumns == queryContext.getSelectColumnCount(), "Unexpected column count");

            for (int i = 0; i < numColumns; i++) {
                int j = i + 1;
                String rsColumn = rsMetadata.getColumnName(j);
                Type rsType = Metadata.prestoToPrimativeType(rsMetadata.getColumnTypeName(j));

                ResolvedColumn resolvedColumn = queryContext.getSelectColumn(i);
                Optional<String> alias = resolvedColumn.getQueryField().getAlias();
                Field resolvedField = resolvedColumn.getResolvedField();
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
                        resolvedField.getType().equals(rsType.toString()),
                        format(
                                "Expected column %d type %s not %s",
                                j, resolvedField.getType(), rsType.toString()));
            }

            return queryContext.getSelectFields();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String toSQL(SearchQueryField field) {
        StringBuilder sql = new StringBuilder();

        sql.append(
                FieldReferenceTransformer.createTransformer(field.getFieldReference(), queryContext)
                        .toSql());
        field.getAlias()
                .ifPresent(
                        alias -> {
                            sql.append(" AS \"");
                            sql.append(alias);
                            sql.append("\"");
                        });

        return sql.toString();
    }

    private String generateFromTable(SearchQueryTable tableAlias) {
        Optional<String> alias = tableAlias.getAlias();
        if (Metadata.DEMO_VIEW.equals(tableAlias.getTableName())) {
            return alias.isPresent()
                    ? Metadata.DEMO_VIEW + " AS \"" + alias.get() + "\""
                    : Metadata.DEMO_VIEW;
        } else {
            PrestoTable prestoTable = metadata.getPrestoTable(tableAlias.getTableName());
            String prestoTableName = prestoTable.getQualifiedName();
            return alias.isPresent()
                    ? prestoTableName + " AS \"" + alias.get() + "\""
                    : prestoTableName;
        }
    }

    private String generateFrom() {
        // TODO: maybe remove this constraint after we support literals in SELECT clause
        checkArgument(!query.getFrom().isEmpty(), "FROM clause cannot be empty");
        return Joiner.on(", ")
                .join(query.getFrom().stream().map(t -> generateFromTable(t)).collect(toList()));
    }
}
