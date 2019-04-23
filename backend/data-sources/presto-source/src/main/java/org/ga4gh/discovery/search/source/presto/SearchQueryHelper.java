package org.ga4gh.discovery.search.source.presto;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.ga4gh.discovery.search.Type;
import org.ga4gh.discovery.search.query.And;
import org.ga4gh.discovery.search.query.Equals;
import org.ga4gh.discovery.search.query.Expression;
import org.ga4gh.discovery.search.query.FieldReference;
import org.ga4gh.discovery.search.query.LiteralExpression;
import org.ga4gh.discovery.search.query.Predicate;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.query.SearchQueryField;
import org.ga4gh.discovery.search.query.SearchQueryTable;

public class SearchQueryHelper {

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
}
