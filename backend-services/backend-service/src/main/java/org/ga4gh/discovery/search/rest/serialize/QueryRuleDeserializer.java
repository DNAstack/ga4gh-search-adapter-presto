/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.rest.serialize;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.ga4gh.discovery.search.query.QueryCondition;
import org.ga4gh.discovery.search.query.QueryRule;
import org.ga4gh.discovery.search.query.QueryRuleSet;
import org.ga4gh.discovery.search.query.QuerySingleRule;

/**
 *
 * @author mfiume
 */
public class QueryRuleDeserializer extends JsonDeserializer<QueryRule> {

    @Override
    public QueryRule deserialize(JsonParser jp, DeserializationContext dc) throws IOException, JsonProcessingException {
        ObjectCodec codec = jp.getCodec();
        JsonNode node = codec.readTree(jp);
        return deserializeFromNode(node);
    }

    private QuerySingleRule deserializeSingleRule(JsonNode node) {
        return new QuerySingleRule(
                node.get("field").asText(), 
                node.get("operator").asText(), 
                node.get("value").asText());
    }

    private QueryRuleSet deserializeRuleSet(JsonNode node) {
        List<QueryRule> rules = new ArrayList<>();
        
        if (node.get("rules") != null) {
            for (final JsonNode ruleNode : node.get("rules")) {
                rules.add(deserializeFromNode(ruleNode));
            }
        }

        return new QueryRuleSet(
                new QueryCondition(node.get("condition").asText()), 
                rules);
    }

    private QueryRule deserializeFromNode(JsonNode node) {
        if (node.get("field") != null) {
            return deserializeSingleRule(node);
        } else if (node.get("condition") != null) {
            return deserializeRuleSet(node);
        } else {
            return new QueryRuleSet();
        }
    }
}