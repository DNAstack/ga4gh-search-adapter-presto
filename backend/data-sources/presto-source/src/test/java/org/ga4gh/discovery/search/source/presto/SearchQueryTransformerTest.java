package org.ga4gh.discovery.search.source.presto;

import static org.ga4gh.discovery.search.source.presto.MockPrestoMetadata.standardMetadata;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.and;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.field;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.fieldRef;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.from;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.literalNum;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.literalStr;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.noLimit;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.noOffset;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.select;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.tableAs;
import static org.ga4gh.discovery.search.source.presto.SearchQueryHelper.where;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.ga4gh.discovery.search.query.Predicate;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.query.SearchQueryField;
import org.ga4gh.discovery.search.query.SearchQueryTable;
import org.junit.Test;

public class SearchQueryTransformerTest {

    private SearchQueryTransformer transformer;

    @Test
    public void testTransform() {
        givenQuery(
                select(field("var.reference_name"), field("var.reference_base")),
                from(tableAs("variants", "var")),
                where(
                        and(
                                SearchQueryHelper.equals(
                                        fieldRef("var.alternate_base"), literalStr("A")),
                                SearchQueryHelper.equals(
                                        fieldRef("var.start_position"), literalNum("100")))),
                noLimit(),
                noOffset());

        assertThat(
                transformer.toPrestoSQL(),
                is(
                        "SELECT \"var\".\"reference_name\", \"var\".\"reference_base\"\n"
                                + "FROM \"bigquery-pgc-data\".\"pgp_variants\".\"view_variants1_beacon\" AS \"var\"\n"
                                + "WHERE \"var\".\"alternate_base\" = 'A' AND \"var\".\"start_position\" = 100"));
    }

    @Test
    public void testDemoViewQuery() {
        givenQuery(SearchQueryHelper.demoViewQuery());
        System.out.println(transformer.toPrestoSQL());
        assertThat(
                transformer.toPrestoSQL(),
                is(
                        "SELECT \"fac\".\"participant_id\" AS \"participant_id\", "
                                + "\"var\".\"reference_name\" AS \"chromosome\", "
                                + "\"var\".\"start_position\" AS \"start_position\", "
                                + "\"var\".\"end_position\" AS \"end_position\", "
                                + "\"var\".\"reference_base\" AS \"reference_base\", "
                                + "\"var\".\"alternate_base\" AS \"alternate_base\", "
                                + "\"drs\".\"size\" AS \"vcf_size\", "
                                + "\"drs\".\"urls\" AS \"vcf_urls\", "
                                + "\"fac\".\"category\" AS \"category\", "
                                + "\"fac\".\"key\" AS \"key\", "
                                + "\"fac\".\"raw_value\" AS \"raw_value\", "
                                + "\"fac\".\"numeric_value\" AS \"numeric_value\"\n"
                                + "FROM \"drs\".\"org_ga4gh_drs\".\"objects\" AS \"drs\", "
                                + "\"bigquery-pgc-data\".\"pgp_variants\".\"view_variants1_beacon\" AS \"var\", "
                                + "\"postgres\".\"public\".\"fact\" AS \"fac\", "
                                + "\"postgres\".\"public\".\"fact\" AS \"f_drs\", "
                                + "\"postgres\".\"public\".\"fact\" AS \"f_var\"\n"
                                + "WHERE \"f_drs\".\"participant_id\" = \"fac\".\"participant_id\" "
                                + "AND \"f_var\".\"participant_id\" = \"fac\".\"participant_id\" "
                                + "AND \"f_var\".\"raw_value\" = \"var\".\"call_name\" "
                                + "AND \"drs\".\"id\" = \"f_drs\".\"raw_value\" "
                                + "AND \"f_drs\".\"key\" = 'Source VCF object ID' "
                                + "AND \"f_drs\".\"category\" = 'Profile' "
                                + "AND \"f_var\".\"key\" = 'Variant call name' "
                                + "AND \"f_var\".\"category\" = 'Profile'"));
    }

    private void givenQuery(
            List<SearchQueryField> select,
            List<SearchQueryTable> from,
            Optional<Predicate> where,
            OptionalLong limit,
            OptionalLong offset) {
        givenQuery(new SearchQuery(select, from, where, limit, offset));
    }

    private void givenQuery(SearchQuery query) {
        PrestoAdapter presto = new MockPrestoAdapter(standardMetadata());
        PrestoMetadata prestoMetadata = new PrestoMetadata(presto);
        Metadata metadata = new Metadata(prestoMetadata);
        QueryContext queryContext = new QueryContext(query, metadata);
        transformer = new SearchQueryTransformer(metadata, query, queryContext);
    }
}
