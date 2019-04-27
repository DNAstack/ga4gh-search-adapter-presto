package org.ga4gh.discovery.search.test.model;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.ga4gh.discovery.search.test.model.Type;
import org.ga4gh.discovery.search.test.model.PredicateDeserializer;
import org.ga4gh.discovery.search.test.model.SearchQueryDeserializer;
import org.ga4gh.discovery.search.test.model.SearchQuerySerializer;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class SearchQueryHelper {

    public static SearchQuery exampleQuery() {
        return query(
                select(
                        field("participant_id"),
                        field("category"),
                        field("key"),
                        field("raw_value"),
                        field("vcf_object")),
                from(table("pgp_canada")),
                where(
                        and(
                                equals(fieldRef("chromosome"), literalStr("chr1")),
                                equals(fieldRef("start_position"), literalNum("5087263")),
                                equals(fieldRef("reference_base"), literalStr("A")),
                                equals(fieldRef("alternate_base"), literalStr("G")))),
                limit(10),
                noOffset());
    }

    /** Generates the demo_view definition. */
    public static SearchQuery demoViewQuery() {
        return query(
                select(
                        fieldAs("fac.participant_id", "participant_id"),
                        fieldAs("var.reference_name", "chromosome"),
                        fieldAs("var.start_position", "start_position"),
                        fieldAs("var.end_position", "end_position"),
                        fieldAs("var.reference_base", "reference_base"),
                        fieldAs("var.alternate_base", "alternate_base"),
                        fieldAs("drs.size", "vcf_size"),
                        fieldAs("drs2.json", "vcf_object"),
                        fieldAs("fac.category", "category"),
                        fieldAs("fac.key", "key"),
                        fieldAs("fac.raw_value", "raw_value"),
                        fieldAs("fac.numeric_value", "numeric_value")),
                from(
                        tableAs("files", "drs"),
                        tableAs("files_json", "drs2"),
                        tableAs("variants", "var"),
                        tableAs("facts", "fac"),
                        tableAs("facts", "f_drs"),
                        tableAs("facts", "f_var")),
                where(
                        and(
                                equals(
                                        fieldRef("f_drs.participant_id"),
                                        fieldRef("fac.participant_id")),
                                equals(
                                        fieldRef("f_var.participant_id"),
                                        fieldRef("fac.participant_id")),
                                equals(fieldRef("f_var.raw_value"), fieldRef("var.call_name")),
                                equals(fieldRef("drs.id"), fieldRef("f_drs.raw_value")),
                                equals(fieldRef("drs2.id"), fieldRef("drs.id")),
                                equals(fieldRef("f_drs.key"), literalStr("Source VCF object ID")),
                                equals(fieldRef("f_drs.category"), literalStr("Profile")),
                                equals(fieldRef("f_var.key"), literalStr("Variant call name")),
                                equals(fieldRef("f_var.category"), literalStr("Profile")))),
                noLimit(),
                noOffset());
    }

    public static SearchQuery query(
            List<SearchQueryField> select,
            List<SearchQueryTable> from,
            Optional<Predicate> where,
            OptionalLong limit,
            OptionalLong offset) {
        return new SearchQuery(select, from, where, limit, offset);
    }

    public static LiteralExpression literalStr(String value) {
        return new LiteralExpression(value, Type.STRING);
    }

    public static LiteralExpression literalNum(String value) {
        return new LiteralExpression(value, Type.NUMBER);
    }

    public static FieldReference fieldRef(String reference) {
        return FieldReference.parse(reference);
    }

    public static Predicate equals(Expression leftExpression, Expression rightExpression) {
        return new Equals(leftExpression, rightExpression);
    }

    public static Predicate and(Predicate... predicates) {
        return new And(Arrays.asList(predicates));
    }

    public static SearchQueryField fieldAs(String fieldRef, String alias) {
        return new SearchQueryField(FieldReference.parse(fieldRef), Optional.of(alias));
    }

    public static SearchQueryField field(String fieldRef) {
        return new SearchQueryField(FieldReference.parse(fieldRef), Optional.empty());
    }

    public static SearchQueryTable table(String tableName) {
        return new SearchQueryTable(tableName, Optional.empty());
    }

    public static SearchQueryTable tableAs(String tableName, String alias) {
        return new SearchQueryTable(tableName, Optional.of(alias));
    }

    public static List<SearchQueryField> select(SearchQueryField... fields) {
        return Arrays.asList(fields);
    }

    public static List<SearchQueryTable> from(SearchQueryTable... tables) {
        return Arrays.asList(tables);
    }

    public static Optional<Predicate> where(Predicate predicate) {
        return Optional.of(predicate);
    }

    public static Optional<Predicate> noWhere() {
        return Optional.empty();
    }

    public static OptionalLong limit(int limit) {
        return OptionalLong.of(limit);
    }

    public static OptionalLong noLimit() {
        return OptionalLong.empty();
    }

    public static OptionalLong noOffset() {
        return OptionalLong.empty();
    }

    public static ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();

        SimpleModule module =
                new SimpleModule("TestSerialization", new Version(1, 0, 0, null, null, null));
        module.addDeserializer(Predicate.class, new PredicateDeserializer());
        module.addDeserializer(SearchQuery.class, new SearchQueryDeserializer());
        module.addSerializer(SearchQuery.class, new SearchQuerySerializer());
        objectMapper.registerModule(module);

        return objectMapper;
    }

    public static SearchQuery deserialize(InputStream jsonStream) throws IOException {
        return objectMapper().readValue(jsonStream, SearchQuery.class);
    }

    public static String serializeToString(SearchQuery query) throws IOException {
        return objectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(query);
    }
}
