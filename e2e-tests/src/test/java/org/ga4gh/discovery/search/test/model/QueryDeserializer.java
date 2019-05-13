package org.ga4gh.discovery.search.test.model;

import static com.google.common.base.Preconditions.checkArgument;
import static org.ga4gh.discovery.search.test.model.SearchQueryHelper.joinListToTree;
import static org.ga4gh.discovery.search.test.model.ExpressionDeserializer.expressionFromNode;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GroupBy;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Offset;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QueryBody;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.With;

public class QueryDeserializer extends JsonDeserializer<Query> {

    @Override
    public Query deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        ObjectCodec codec = jp.getCodec();

        // these are not supported yet
        Optional<With> with = Optional.empty();
        Optional<OrderBy> orderBy = Optional.empty();

        // these are not supported at this location
        Optional<Offset> offset = Optional.empty();
        Optional<Node> limit = Optional.empty();

        return new Query(with, parseQueryBody(codec.readTree(jp)), orderBy, offset, limit);
    }

    private QueryBody parseQueryBody(JsonNode node) {
        checkArgument(node.has("select"), "Missing select clause");
        checkArgument(node.has("from"), "Missing from clause");
        Select select = parseSelectClause(node.get("select"));
        Optional<Relation> from = parseFromClause(node.get("from"));
        Optional<Expression> where =
                node.has("where") ? parseWhereClause(node.get("where")) : Optional.empty();
        Optional<Offset> offset =
                node.has("offset") ? parseOffset(node.get("offset")) : Optional.empty();
        Optional<Node> limit = node.has("limit") ? parseLimit(node.get("limit")) : Optional.empty();

        // these are not supported yet
        Optional<GroupBy> groupBy = Optional.empty();
        Optional<Expression> having = Optional.empty();
        Optional<OrderBy> orderBy = Optional.empty();

        return new QuerySpecification(select, from, where, groupBy, having, orderBy, offset, limit);
    }

    private Select parseSelectClause(JsonNode node) {
        return new Select(false, parseSelectItems(node));
    }

    private List<SelectItem> parseSelectItems(JsonNode selectNode) {
        checkArgument(selectNode.isArray(), "select clause needs to be an array");
        ImmutableList.Builder<SelectItem> listBuilder = ImmutableList.builder();
        for (JsonNode field : selectNode) {
            checkArgument(field.isObject(), "select clause can only contain field alias objects");
            listBuilder.add(parseSelectColumn(field));
        }
        return listBuilder.build();
    }

    private SingleColumn parseSelectColumn(JsonNode node) {
        checkArgument(node.has("field"), "No field reference found");
        Optional<Identifier> alias =
                node.has("alias")
                        ? Optional.of(new Identifier(node.get("alias").asText()))
                        : Optional.empty();
        Expression expression = ExpressionDeserializer.parseField(node.get("field").asText());
        return new SingleColumn(expression, alias);
    }

    private Optional<Relation> parseFromClause(JsonNode node) {
        List<Relation> relations = parseFromTables(node);
        if (relations.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(joinListToTree(relations));
        }
    }

    private List<Relation> parseFromTables(JsonNode fromNode) {
        checkArgument(fromNode.isArray(), "from clause needs to be an array");
        ImmutableList.Builder<Relation> listBuilder = ImmutableList.builder();
        for (JsonNode field : fromNode) {
            checkArgument(field.isObject(), "from clause can only contain table alias objects");
            listBuilder.add(parseFromTable(field));
        }
        return listBuilder.build();
    }

    private Relation parseFromTable(JsonNode node) {
        checkArgument(node.has("table"), "No table id found");
        // TODO: multi-part names
        Table table = new Table(QualifiedName.of(node.get("table").asText()));
        if (node.has("alias")) {
            return new AliasedRelation(table, new Identifier(node.get("alias").asText()), null);
        } else {
            return table;
        }
    }

    private Optional<Expression> parseWhereClause(JsonNode node) {
        return Optional.of(expressionFromNode(node));
    }

    private Optional<Offset> parseOffset(JsonNode node) {
        return Optional.of(new Offset(node.asText()));
    }

    private Optional<Node> parseLimit(JsonNode node) {
        return Optional.of(new Limit(node.asText()));
    }
}
