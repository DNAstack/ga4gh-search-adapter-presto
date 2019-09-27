//package com.dnastack.ga4gh.search.adapter.source.presto;
//
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.and;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.field;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.fieldRef;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.from;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.literalNum;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.literalStr;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.noLimit;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.noOffset;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.select;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.tableAs;
//import static com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper.where;
//import static com.dnastack.ga4gh.search.adapter.source.presto.MockPrestoMetadata.standardMetadata;
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.is;
//
//import java.util.Optional;
//
//import com.dnastack.ga4gh.search.adapter.query.SearchQueryHelper;
//import org.junit.Test;
//
//import io.prestosql.sql.tree.Expression;
//import io.prestosql.sql.tree.Node;
//import io.prestosql.sql.tree.Offset;
//import io.prestosql.sql.tree.Query;
//import io.prestosql.sql.tree.Relation;
//import io.prestosql.sql.tree.Select;
//
//public class SearchQueryTransformerTest {
//
//    private SearchQueryTransformer transformer;
//
//    @Test
//    public void testTransform() {
//        givenQuery(
//                select(field("var.reference_name"), field("var.reference_base")),
//                from(tableAs("variants", "var")),
//                where(
//                        and(
//                                SearchQueryHelper.equals(
//                                        fieldRef("var.alternate_base"), literalStr("A")),
//                                SearchQueryHelper.equals(
//                                        fieldRef("var.start_position"), literalNum("100")))),
//                noOffset(),
//                noLimit());
//
//        assertThat(
//                transformer.toPrestoSQL(),
//                is(
//                        "SELECT \"var\".\"reference_name\", \"var\".\"reference_base\"\n"
//                                + "FROM \"bigquery-pgc-data\".\"pgp_variants\".\"view_variants2_beacon\" AS \"var\"\n"
//                                + "WHERE ((\"var\".\"alternate_base\" = 'A') AND (\"var\".\"start_position\" = DECIMAL '100'))"));
//    }
//
//    @Test
//    public void testDemoViewQuery() {
//        givenQuery(SearchQueryHelper.demoViewQuery());
//        // System.out.println(transformer.toPrestoSQL());
//        String expectedSQL =
//                "SELECT \"fac\".\"participant_id\" AS \"participant_id\", "
//                        + "\"var\".\"reference_name\" AS \"chromosome\", "
//                        + "\"var\".\"start_position\" AS \"start_position\", "
//                        + "\"var\".\"end_position\" AS \"end_position\", "
//                        + "\"var\".\"reference_base\" AS \"reference_base\", "
//                        + "\"var\".\"alternate_base\" AS \"alternate_base\", "
//                        + "\"drs\".\"size\" AS \"vcf_size\", "
//                        + "\"drs2\".\"json\" AS \"vcf_object\", "
//                        + "\"fac\".\"category\" AS \"category\", "
//                        + "\"fac\".\"key\" AS \"key\", "
//                        + "\"fac\".\"raw_value\" AS \"raw_value\", "
//                        + "\"fac\".\"numeric_value\" AS \"numeric_value\"\n"
//                        + "FROM \"drs\".\"org_ga4gh_drs\".\"objects\" AS \"drs\",\n"
//                        + "\"drs\".\"org_ga4gh_drs\".\"json_objects\" AS \"drs2\",\n"
//                        + "\"bigquery-pgc-data\".\"pgp_variants\".\"view_variants2_beacon\" AS \"var\",\n"
//                        + "\"postgres\".\"public\".\"fact\" AS \"fac\",\n"
//                        + "\"postgres\".\"public\".\"fact\" AS \"f_drs\",\n"
//                        + "\"postgres\".\"public\".\"fact\" AS \"f_var\"\n"
//                        + "WHERE (((((((((\"f_drs\".\"participant_id\" = \"fac\".\"participant_id\") AND (\"f_var\".\"participant_id\" = \"fac\".\"participant_id\")) AND (\"f_var\".\"raw_value\" = \"var\".\"call_name\")) AND (\"drs\".\"id\" = \"f_drs\".\"raw_value\")) AND (\"drs2\".\"id\" = \"drs\".\"id\")) AND (\"f_drs\".\"key\" = 'Source VCF object ID')) AND (\"f_drs\".\"category\" = 'Profile')) AND (\"f_var\".\"key\" = 'Variant call name')) AND (\"f_var\".\"category\" = 'Profile'))";
//        //        System.out.println("==============");
//        //        System.out.println(expectedSQL);
//
//        assertThat(transformer.toPrestoSQL(), is(expectedSQL));
//    }
//
//    private void givenQuery(
//            Select select,
//            Optional<Relation> from,
//            Optional<Expression> where,
//            Optional<Offset> offset,
//            Optional<Node> limit) {
//        givenQuery(SearchQueryHelper.query(select, from, where, offset, limit));
//    }
//
//    private void givenQuery(Query query) {
//        PrestoAdapter presto = new MockPrestoAdapter(standardMetadata());
//        PrestoMetadata prestoMetadata = new PrestoMetadata(presto);
//        Metadata metadata = new Metadata(prestoMetadata);
//        QueryContext queryContext = new QueryContext(query, metadata);
//        transformer = new SearchQueryTransformer(metadata, query, queryContext);
//    }
//}
