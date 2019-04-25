package org.ga4gh.discovery.search.test.model;

import static com.google.common.base.Preconditions.checkArgument;
import static org.ga4gh.discovery.search.test.model.PredicateDeserializer.predicateFromNode;
import java.util.List;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class And extends Predicate {

    public static final String KEY = "and";

    private final List<Predicate> predicates;

    @Override
    public String getKey() {
        return KEY;
    }

    public static And fromNode(JsonNode node) {
        checkArgument(KEY.equals(node.get("p").asText()));
        ImmutableList.Builder<Predicate> predicateListBuilder = ImmutableList.builder();
        for (final JsonNode predicateNode : node.get("predicates")) {
            predicateListBuilder.add(predicateFromNode(predicateNode));
        }
        return new And(predicateListBuilder.build());
    }

    @Override
    public String toString() {
        return "and(" + predicates + ")";
    }
}
