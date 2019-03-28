package org.ga4gh.discovery.search.source.presto;

import static java.util.stream.Collectors.toList;
import java.util.List;
import org.ga4gh.discovery.search.query.And;
import org.ga4gh.discovery.search.query.Predicate;
import com.google.common.base.Joiner;

public class AndTransformer extends PredicateTransformer {

    protected AndTransformer(Predicate predicate, QueryContext context) {
        super(predicate, context);
    }

    @Override
    public String toSql() {
        And and = (And) super.getPredicate();
        List<Predicate> subPredicates = and.getPredicates();
        return Joiner.on(" AND ")
                .join(
                        subPredicates.stream()
                                .map(
                                        predicate ->
                                                createTransformer(predicate, getQueryContext())
                                                        .toSql())
                                .collect(toList()));
    }
}
