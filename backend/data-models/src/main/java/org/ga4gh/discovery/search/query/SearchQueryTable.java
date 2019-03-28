package org.ga4gh.discovery.search.query;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Optional;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SearchQueryTable {

    private final String tableName;
    private final Optional<String> alias;

    public static SearchQueryTable fromNode(JsonNode node) {
        checkArgument(node.has("table"), "No table id found");
        Optional<String> alias =
                node.has("alias") ? Optional.of(node.get("alias").asText()) : Optional.empty();
        return new SearchQueryTable(node.get("table").asText(), alias);
    }
}
