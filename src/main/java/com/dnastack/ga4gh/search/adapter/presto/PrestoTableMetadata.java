package com.dnastack.ga4gh.search.adapter.presto;

import java.util.List;
import java.util.Optional;

import com.dnastack.ga4gh.search.adapter.model.Field;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PrestoTableMetadata {

    private final PrestoTable table;
    private final List<Field> fields;

    public Optional<Field> getField(String fieldName) {
        return fields.stream().filter(f -> f.getName().equals(fieldName)).findAny();
    }
}
