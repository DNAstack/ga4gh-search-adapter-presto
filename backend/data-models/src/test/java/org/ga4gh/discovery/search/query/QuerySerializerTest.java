package org.ga4gh.discovery.search.query;

import static org.ga4gh.discovery.search.query.SearchQueryHelper.deserialize;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.deserializeFromString;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.exampleQuery;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.serializeToString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import io.prestosql.sql.tree.Query;

public class QuerySerializerTest {

    @Test
    public void testSerialization() throws IOException {
        Query query = deserializeFromFile("/query.json");
        Query queryAfterRoundtrip = deserializeFromString(serializeToString(query));
        assertEquals(query, queryAfterRoundtrip);
    }

    @Test
    public void testSerializationExample() throws IOException {
        Query query = exampleQuery();
        String serializedQuery = serializeToString(query);
        Query queryAfterRoundtrip = deserializeFromString(serializedQuery);
        assertEquals(query, queryAfterRoundtrip);
    }

    private Query deserializeFromFile(String file) throws IOException {
        InputStream jsonStream = this.getClass().getResourceAsStream(file);
        assertThat(jsonStream, notNullValue());
        return deserialize(jsonStream);
    }
}
