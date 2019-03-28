package org.ga4gh.discovery.search.rest;

import java.util.List;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.source.SearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FieldsController {

    @Autowired SearchSource dataSource;

    @RequestMapping("/api/fields")
    public List<Field> fields(@RequestParam String table) {
        return dataSource.getFields(table);
    }
}
