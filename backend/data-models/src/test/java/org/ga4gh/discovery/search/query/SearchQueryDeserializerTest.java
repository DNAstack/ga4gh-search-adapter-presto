package org.ga4gh.discovery.search.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.ga4gh.discovery.search.serde.PredicateDeserializer;
import org.ga4gh.discovery.search.serde.SearchQueryDeserializer;
import org.junit.Test;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class SearchQueryDeserializerTest {

    @Test
    public void testDeserialization() throws IOException {
        SearchQuery query = deserializeFromFile("/query.json");
        List<SearchQueryField> select = query.getSelect();
        List<SearchQueryTable> from = query.getFrom();
        assertThat(select, notNullValue());
        assertThat(from, notNullValue());
        assertThat(query.getWhere(), notNullValue());

        assertThat(select, hasSize(10));

        assertThat(select.get(0).getFieldReference().toString(), is("fac.participant_id"));
        assertThat(select.get(1).getFieldReference().toString(), is("var.reference_name"));
        assertThat(select.get(2).getFieldReference().toString(), is("var.start_position"));
        assertThat(select.get(3).getFieldReference().toString(), is("var.reference_base"));
        assertThat(select.get(4).getFieldReference().toString(), is("var.alternate_base"));
        assertThat(select.get(5).getFieldReference().toString(), is("drs.size"));
        assertThat(select.get(6).getFieldReference().toString(), is("drs.urls"));
        assertThat(select.get(7).getFieldReference().toString(), is("fac.category"));
        assertThat(select.get(8).getFieldReference().toString(), is("fac.key"));
        assertThat(select.get(9).getFieldReference().toString(), is("fac.raw_value"));

        assertThat(select.get(0).getAlias().get(), is("pcpt_id"));
        assertThat(select.get(1).getAlias().get(), is("chromosome"));
        assertThat(select.get(2).getAlias().get(), is("start_pos"));
        assertThat(select.get(3).getAlias().get(), is("ref_base"));
        assertThat(select.get(4).getAlias().get(), is("alt_base"));
        assertThat(select.get(5).getAlias().get(), is("vcf_size"));
        assertThat(select.get(6).getAlias().get(), is("vcf_urls"));
        assertThat(select.get(7).getAlias().get(), is("category"));
        assertThat(select.get(8).getAlias().get(), is("key"));
        assertThat(select.get(9).getAlias().get(), is("value"));

        assertThat(from, hasSize(5));

        assertThat(from.get(0).getTableName(), is("files"));
        assertThat(from.get(1).getTableName(), is("variants"));
        assertThat(from.get(2).getTableName(), is("facts"));
        assertThat(from.get(3).getTableName(), is("facts"));
        assertThat(from.get(4).getTableName(), is("facts"));

        assertThat(from.get(0).getAlias().get(), is("drs"));
        assertThat(from.get(1).getAlias().get(), is("var"));
        assertThat(from.get(2).getAlias().get(), is("fac"));
        assertThat(from.get(3).getAlias().get(), is("f_drs"));
        assertThat(from.get(4).getAlias().get(), is("f_var"));
    }

    private SearchQuery deserializeFromFile(String file) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        InputStream jsonStream = this.getClass().getResourceAsStream(file);
        assertThat(jsonStream, notNullValue());

        SimpleModule module =
                new SimpleModule("TestSerialization", new Version(1, 0, 0, null, null, null));
        module.addDeserializer(Predicate.class, new PredicateDeserializer());
        module.addDeserializer(SearchQuery.class, new SearchQueryDeserializer());
        objectMapper.registerModule(module);
        return objectMapper.readValue(jsonStream, SearchQuery.class);
    }
}
