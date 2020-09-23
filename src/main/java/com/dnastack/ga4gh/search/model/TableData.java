package com.dnastack.ga4gh.search.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TableData {

    @JsonProperty("data_model")
    private DataModel dataModel;

    @JsonProperty("data")
    private List<Map<String, Object>> data;

    @JsonProperty("pagination")
    private Pagination pagination;

    private static <T> List<T> concat(List<T> l1, List<T> l2) {
        if (l1 != null && l2 != null) {
            List<T> result = new ArrayList<>(l1.size() + l2.size());
            result.addAll(l1);
            result.addAll(l2);
            return result;
        } else if (l1 != null) {
            return List.copyOf(l1);
        } else if (l2 != null) {
            return List.copyOf(l2);
        } else {
            return null;
        }
    }

    public void append(TableData tableData) {

        if (tableData.getData() != null) {
            this.data = concat(this.data, tableData.getData());
        }
        if (tableData.getDataModel() != null) {
            this.dataModel = tableData.getDataModel();
        }
        this.pagination = tableData.getPagination();

    }

}
