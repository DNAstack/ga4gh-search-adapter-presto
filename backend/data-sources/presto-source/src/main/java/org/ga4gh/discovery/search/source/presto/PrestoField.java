package org.ga4gh.discovery.search.source.presto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PrestoField {

    private final String name;
    private final String type;
}
