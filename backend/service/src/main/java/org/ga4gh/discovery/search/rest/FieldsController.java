package org.ga4gh.discovery.search.rest;

import java.util.List;
import org.ga4gh.discovery.search.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.ga4gh.discovery.search.source.SearchSource;

@RestController
public class FieldsController {

    @Autowired
    SearchSource dataSource;
    
    @CrossOrigin(origins = "${cors.url}")
    @RequestMapping("/fields")
    public List<Field> fields() {
        return dataSource.getFields();
    }
}
