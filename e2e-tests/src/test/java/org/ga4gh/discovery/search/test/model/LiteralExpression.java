package org.ga4gh.discovery.search.test.model;

import org.ga4gh.discovery.search.test.model.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class LiteralExpression extends Expression {

    private final String value;
    private final Type type;

    @Override
    public String toString() {
        return value;
    }
}
