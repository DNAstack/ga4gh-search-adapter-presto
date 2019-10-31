package com.dnastack.ga4gh.search.adapter.presto;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import com.dnastack.ga4gh.search.adapter.model.Field;

@Getter
@AllArgsConstructor
@EqualsAndHashCode(of = {"table"})
public class TableMetadata {

    private final TableReference table;
    private final List<Field> fields;

    public String getTableName() {
        return table.getName();
    }

    public String getTableSchema() {
        return table.getSchema();
    }

    public boolean hasField(String fieldName) {
        return findField(fieldName).isPresent();
    }

    public Field getField(String fieldName) {
        Optional<Field> field = findField(fieldName);
        checkArgument(
                field.isPresent(),
                format(
                        "Table %s with schema %s doesn't contain field %s",
                        table.getName(), table.getSchema(), fieldName));
        return field.get();
    }

    public Optional<Field> findField(String fieldName) {
        return fields.stream().filter(f -> f.getName().equals(fieldName)).findAny();
    }

    @Override
    public String toString() {
        return table.toString();
    }
}
