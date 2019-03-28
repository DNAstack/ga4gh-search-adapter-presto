package org.ga4gh.discovery.search.source.presto;

import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PrestoTableMetadata {

    private final PrestoTable table;
    private final List<PrestoField> fields;

    public boolean hasField(String fieldName) {
        return getField(fieldName).isPresent();
    }

    public Optional<PrestoField> getField(String fieldName) {
        return fields.stream().filter(f -> f.getName().equals(fieldName)).findAny();
    }
}
