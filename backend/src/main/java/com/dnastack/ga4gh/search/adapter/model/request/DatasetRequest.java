package com.dnastack.ga4gh.search.adapter.model.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DatasetRequest {

    private String url;
    private Integer page;
    private Integer pageSize;
    private String id;
}
