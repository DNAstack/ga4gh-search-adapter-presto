package org.ga4gh.discovery.search.query;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class FieldReference extends Expression {

    private final Optional<String> tableReference;
    private final String fieldName;

    public static FieldReference parse(String reference) {
        String[] fieldTuple = reference.split("\\.");
        checkArgument(fieldTuple.length < 3, "Wrong field reference format: " + reference);
        if (fieldTuple.length == 1) {
            return new FieldReference(Optional.empty(), fieldTuple[0]);
        } else {
            return new FieldReference(Optional.of(fieldTuple[0]), fieldTuple[1]);
        }
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        tableReference.ifPresent(
                tableRef -> {
                    s.append(tableRef);
                    s.append(".");
                });
        s.append(fieldName);
        return s.toString();
    }
}
