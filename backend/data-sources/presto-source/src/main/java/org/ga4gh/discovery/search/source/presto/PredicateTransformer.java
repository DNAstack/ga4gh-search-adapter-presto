package org.ga4gh.discovery.search.source.presto;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Map;
import java.util.function.BiFunction;
import org.ga4gh.discovery.search.query.And;
import org.ga4gh.discovery.search.query.Equals;
import org.ga4gh.discovery.search.query.Predicate;
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class PredicateTransformer {

    private static final Map<String, BiFunction<Predicate, QueryContext, PredicateTransformer>>
            CONSTRUCTORS =
                    ImmutableMap
                            .<String, BiFunction<Predicate, QueryContext, PredicateTransformer>>
                                    builder()
                            .put(And.KEY, AndTransformer::new)
                            .put(Equals.KEY, EqualsTransformer::new)
                            //                    .put(Like.KEY, Like::fromNode)
                            .build();

    private final Predicate predicate;
    private final QueryContext queryContext;

    public abstract String toSql();

    public static PredicateTransformer createTransformer(
            Predicate predicate, QueryContext queryContext) {
        BiFunction<Predicate, QueryContext, PredicateTransformer> constructor =
                CONSTRUCTORS.get(predicate.getKey());
        checkArgument(constructor != null, "Unknown perdicate type: " + predicate.getKey());
        return constructor.apply(predicate, queryContext);
    }
}
