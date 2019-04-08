package org.ga4gh.discovery.search.serde;

import static com.google.common.base.Preconditions.checkArgument;
import static org.ga4gh.discovery.search.serde.PredicateDeserializer.predicateFromNode;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.ga4gh.discovery.search.query.Predicate;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.query.SearchQueryField;
import org.ga4gh.discovery.search.query.SearchQueryTable;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

public class SearchQueryDeserializer extends JsonDeserializer<SearchQuery> {

    @Override
    public SearchQuery deserialize(JsonParser jp, DeserializationContext dc)
            throws IOException, JsonProcessingException {
        ObjectCodec codec = jp.getCodec();
        JsonNode node = codec.readTree(jp);
        checkArgument(node.has("select"), "Missing select clause");
        checkArgument(node.has("from"), "Missing from clause");
        List<SearchQueryField> select = parseSelectClause(node.get("select"));
        List<SearchQueryTable> from = parseFromClause(node.get("from"));
        Optional<Predicate> predicate =
                node.has("where")
                        ? Optional.of(predicateFromNode(node.get("where")))
                        : Optional.empty();
        OptionalLong limit =
                node.has("limit")
                        ? OptionalLong.of(node.get("limit").asLong())
                        : OptionalLong.empty();
        OptionalLong offset =
                node.has("offset")
                        ? OptionalLong.of(node.get("offset").asLong())
                        : OptionalLong.empty();
        return new SearchQuery(select, from, predicate, limit, offset);
    }

    private List<SearchQueryField> parseSelectClause(JsonNode selectNode) {
        checkArgument(selectNode.isArray(), "select clause needs to be an array");
        ImmutableList.Builder<SearchQueryField> listBuilder = ImmutableList.builder();
        for (JsonNode field : selectNode) {
            checkArgument(field.isObject(), "select clause can only contain field alias objects");
            listBuilder.add(SearchQueryField.fromNode(field));
        }
        return listBuilder.build();
    }

    private List<SearchQueryTable> parseFromClause(JsonNode fromNode) {
        checkArgument(fromNode.isArray(), "from clause needs to be an array");
        ImmutableList.Builder<SearchQueryTable> listBuilder = ImmutableList.builder();
        for (JsonNode field : fromNode) {
            checkArgument(field.isObject(), "from clause can only contain table alias objects");
            listBuilder.add(SearchQueryTable.fromNode(field));
        }
        return listBuilder.build();
    }
}
