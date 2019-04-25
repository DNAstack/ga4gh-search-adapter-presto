package org.ga4gh.discovery.search.test.model;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Optional;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SearchQueryField {

    private final FieldReference fieldReference;
    private final Optional<String> alias;

    public static SearchQueryField fromNode(JsonNode node) {
        checkArgument(node.has("field"), "No field reference found");
        Optional<String> alias =
                node.has("alias") ? Optional.of(node.get("alias").asText()) : Optional.empty();
        return new SearchQueryField(FieldReference.parse(node.get("field").asText()), alias);
    }

    public Optional<String> getTableReference() {
        return fieldReference.getTableReference();
    }

    public String getName() {
        return fieldReference.getFieldName();
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(fieldReference.toString());
        alias.ifPresent(
                alias -> {
                    s.append(" AS ");
                    s.append(alias);
                });
        return s.toString();
    }
}
