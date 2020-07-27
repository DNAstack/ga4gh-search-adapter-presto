package com.dnastack.ga4gh.search.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TablesList {

    @JsonProperty("tables")
    private List<TableInfo> tableInfos;

    @JsonProperty("errors")
    private List<TableError> errors;

    @JsonProperty("pagination")
    private Pagination pagination;

    private static <T> List<T> concat(List<T> l1, List<T> l2){
        if(l1 != null && l2 != null){
            List<T> result = new ArrayList<>(l1.size() + l2.size());
            result.addAll(l1);
            result.addAll(l2);
            return result;
        }else if(l1 != null){
            return List.copyOf(l1);
        }else if(l2 != null){
            return List.copyOf(l2);
        }else{
            return null;
        }
    }

    private void append(final TablesList lt){
        this.pagination = lt.pagination;
        this.errors = concat(this.errors, lt.errors);
        this.tableInfos = concat(this.tableInfos, lt.tableInfos);
    }

    public TablesList(List<TablesList> tablesLists){
        this.errors = null;
        this.pagination = null;
        this.tableInfos = List.of();
        for(TablesList tablesList : tablesLists){
            this.append(tablesList);
        }
    }
}
