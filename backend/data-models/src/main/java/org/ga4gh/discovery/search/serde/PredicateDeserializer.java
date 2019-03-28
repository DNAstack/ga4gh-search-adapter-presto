package org.ga4gh.discovery.search.serde;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import org.ga4gh.discovery.search.query.And;
import org.ga4gh.discovery.search.query.Equals;
import org.ga4gh.discovery.search.query.Like;
import org.ga4gh.discovery.search.query.Predicate;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

public class PredicateDeserializer extends JsonDeserializer<Predicate> {

    private static final Map<String, Function<JsonNode, Predicate>> CONSTRUCTORS =
            ImmutableMap.<String, Function<JsonNode, Predicate>>builder()
                    .put(And.KEY, And::fromNode)
                    .put(Equals.KEY, Equals::fromNode)
                    .put(Like.KEY, Like::fromNode)
                    .build();

    @Override
    public Predicate deserialize(JsonParser jp, DeserializationContext dc)
            throws IOException, JsonProcessingException {
        ObjectCodec codec = jp.getCodec();
        return predicateFromNode(codec.readTree(jp));
    }

    public static Predicate predicateFromNode(JsonNode node) {
        checkArgument(node.has("p"), "Predicate has to have the predicate type \"p\"");
        String predicateType = node.get("p").asText();
        Function<JsonNode, Predicate> constructor = CONSTRUCTORS.get(predicateType);
        checkArgument(constructor != null, "Unknown perdicate type: " + predicateType);
        return constructor.apply(node);
    }
}
