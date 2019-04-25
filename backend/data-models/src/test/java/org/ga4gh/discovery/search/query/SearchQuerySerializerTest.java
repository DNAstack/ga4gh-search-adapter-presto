package org.ga4gh.discovery.search.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

public class SearchQuerySerializerTest {

    @Test
    public void testSerialization() throws IOException {
        SearchQuery query = deserializeFromFile("/query.json");
        String jsonQuery = SearchQueryHelper.serializeToString(query);
        System.out.println(jsonQuery);
    }

    private SearchQuery deserializeFromFile(String file) throws IOException {
        InputStream jsonStream = this.getClass().getResourceAsStream(file);
        assertThat(jsonStream, notNullValue());
        return SearchQueryHelper.deserialize(jsonStream);
    }
}
