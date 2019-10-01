package com.dnastack.ga4gh.search.adapter.test.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import org.ga4gh.dataset.model.Pagination;

public class PaginationDeserializer extends JsonDeserializer<Pagination> {

    @Override
    public Pagination deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = p.readValueAsTree();

        if (node != null && !node.isNull()){
            URI prevPage = null;
            URI nextPage = null;

            if (node.has("previous_page_url")) {
                prevPage = URI.create(node.get("previous_page_url").asText());
            }

            if (node.has("next_page_url")){
                nextPage = URI.create(node.get("next_page_url").asText());
            }


            return new Pagination(prevPage,nextPage);
        }

        return null;
    }
}
