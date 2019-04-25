package org.ga4gh.discovery.search.source.presto;

import static org.ga4gh.discovery.search.source.presto.MockPrestoMetadata.standardMetadata;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.and;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.field;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.fieldRef;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.from;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.literalNum;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.literalStr;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.noLimit;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.noOffset;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.select;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.tableAs;
import static org.ga4gh.discovery.search.query.SearchQueryHelper.where;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.ga4gh.discovery.search.query.Predicate;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.query.SearchQueryField;
import org.ga4gh.discovery.search.query.SearchQueryHelper;
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
                                + "FROM \"bigquery-pgc-data\".\"pgp_variants\".\"view_variants2_beacon\" AS \"var\"\n"
                                + "WHERE \"var\".\"alternate_base\" = 'A' AND \"var\".\"start_position\" = 100"));
    }

    @Test
    public void testDemoViewQuery() {
        givenQuery(SearchQueryHelper.demoViewQuery());
        // System.out.println(transformer.toPrestoSQL());
        String expectedSQL =
                "SELECT \"fac\".\"participant_id\" AS \"participant_id\", "
                        + "\"var\".\"reference_name\" AS \"chromosome\", "
                        + "\"var\".\"start_position\" AS \"start_position\", "
                        + "\"var\".\"end_position\" AS \"end_position\", "
                        + "\"var\".\"reference_base\" AS \"reference_base\", "
                        + "\"var\".\"alternate_base\" AS \"alternate_base\", "
                        + "\"drs\".\"size\" AS \"vcf_size\", "
                        + "\"drs2\".\"json\" AS \"vcf_object\", "
                        + "\"fac\".\"category\" AS \"category\", "
                        + "\"fac\".\"key\" AS \"key\", "
                        + "\"fac\".\"raw_value\" AS \"raw_value\", "
                        + "\"fac\".\"numeric_value\" AS \"numeric_value\"\n"
                        + "FROM \"drs\".\"org_ga4gh_drs\".\"objects\" AS \"drs\", "
                        + "\"drs\".\"org_ga4gh_drs\".\"json_objects\" AS \"drs2\", "
                        + "\"bigquery-pgc-data\".\"pgp_variants\".\"view_variants2_beacon\" AS \"var\", "
                        + "\"postgres\".\"public\".\"fact\" AS \"fac\", "
                        + "\"postgres\".\"public\".\"fact\" AS \"f_drs\", "
                        + "\"postgres\".\"public\".\"fact\" AS \"f_var\"\n"
                        + "WHERE \"f_drs\".\"participant_id\" = \"fac\".\"participant_id\" "
                        + "AND \"f_var\".\"participant_id\" = \"fac\".\"participant_id\" "
                        + "AND \"f_var\".\"raw_value\" = \"var\".\"call_name\" "
                        + "AND \"drs\".\"id\" = \"f_drs\".\"raw_value\" "
                        + "AND \"drs2\".\"id\" = \"drs\".\"id\" "
                        + "AND \"f_drs\".\"key\" = 'Source VCF object ID' "
                        + "AND \"f_drs\".\"category\" = 'Profile' "
                        + "AND \"f_var\".\"key\" = 'Variant call name' "
                        + "AND \"f_var\".\"category\" = 'Profile'";
        // System.out.println("==============");
        // System.out.println(expectedSQL);

        assertThat(transformer.toPrestoSQL(), is(expectedSQL));
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
