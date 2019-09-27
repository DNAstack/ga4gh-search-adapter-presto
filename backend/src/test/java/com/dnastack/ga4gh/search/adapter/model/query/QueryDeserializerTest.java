package com.dnastack.ga4gh.search.adapter.model.query;

import static com.dnastack.ga4gh.search.adapter.model.query.SearchQueryHelper.andTreeToList;
import static com.dnastack.ga4gh.search.adapter.model.query.SearchQueryHelper.joinTreeToList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;

import com.google.common.base.Joiner;
import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.Table;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.junit.Test;

public class QueryDeserializerTest {

    private void assertAliasedRelation(Relation rel, String tableName, String alias) {
        AliasedRelation arel = (AliasedRelation) rel;
        Table table = (Table) arel.getRelation();
        assertEquals(tableName, Joiner.on(".").join(table.getName().getParts()));
        assertEquals(alias, arel.getAlias().getValue());
    }

    private void assertSingleColumn(
            SelectItem item, String expectedBase, String expectedField, String alias) {
        SingleColumn col = (SingleColumn) item;
        DereferenceExpression exp = (DereferenceExpression) col.getExpression();
        Identifier base = (Identifier) exp.getBase();
        assertEquals(base.getValue(), expectedBase);
        assertEquals(exp.getField().getValue(), expectedField);
        assertEquals(alias, col.getAlias().get().getValue());
    }

    @Test
    public void testDeserialization() throws IOException {
        Query query = deserializeFromFile("/query.json");
        QuerySpecification querySpec = (QuerySpecification) query.getQueryBody();
        Select select = querySpec.getSelect();
        List<SelectItem> selectItems = select.getSelectItems();
        List<Relation> from = joinTreeToList(querySpec.getFrom().get());
        assertThat(from, notNullValue());
        assertThat(from, hasSize(5));
        assertThat(selectItems, notNullValue());
        assertThat(selectItems, hasSize(10));
        List<Expression> where = andTreeToList(querySpec.getWhere().get());
        assertThat(where, notNullValue());

        assertSingleColumn(selectItems.get(0), "fac", "participant_id", "pcpt_id");
        assertSingleColumn(selectItems.get(1), "var", "reference_name", "chromosome");
        assertSingleColumn(selectItems.get(2), "var", "start_position", "start_pos");
        assertSingleColumn(selectItems.get(3), "var", "reference_base", "ref_base");
        assertSingleColumn(selectItems.get(4), "var", "alternate_base", "alt_base");
        assertSingleColumn(selectItems.get(5), "drs", "size", "vcf_size");
        assertSingleColumn(selectItems.get(6), "drs", "urls", "vcf_urls");
        assertSingleColumn(selectItems.get(7), "fac", "category", "category");
        assertSingleColumn(selectItems.get(8), "fac", "key", "key");
        assertSingleColumn(selectItems.get(9), "fac", "raw_value", "value");

        assertAliasedRelation(from.get(0), "files", "drs");
        assertAliasedRelation(from.get(1), "variants", "var");
        assertAliasedRelation(from.get(2), "facts", "fac");
        assertAliasedRelation(from.get(3), "facts", "f_drs");
        assertAliasedRelation(from.get(4), "facts", "f_var");

        assertEqualsFieldField(where.get(0), "f_drs", "participant_id", "fac", "participant_id");
        assertEqualsFieldField(where.get(1), "f_var", "participant_id", "fac", "participant_id");
        assertEqualsFieldField(where.get(2), "f_var", "raw_value", "var", "call_name");
        assertEqualsFieldField(where.get(3), "drs", "id", "f_drs", "raw_value");
        assertEqualsFieldString(where.get(4), "f_drs", "key", "Source VCF object ID");
        assertEqualsFieldString(where.get(5), "f_drs", "category", "Profile");
        assertEqualsFieldString(where.get(6), "f_var", "key", "Variant call name");
        assertEqualsFieldString(where.get(7), "f_var", "category", "Profile");
        assertEqualsFieldString(where.get(8), "var", "reference_name", "chr1");
        assertEqualsFieldDecimal(where.get(9), "var", "start_position", "5087263");
        assertEqualsFieldString(where.get(10), "var", "reference_base", "A");
        assertEqualsFieldString(where.get(11), "var", "alternate_base", "G");
    }

    private void assertEqualsFieldField(
            Expression expression, String f1base, String f1field, String f2base, String f2field) {
        ComparisonExpression comparison = (ComparisonExpression) expression;
        assertEquals(
                io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL,
                comparison.getOperator());
        DereferenceExpression left = (DereferenceExpression) comparison.getLeft();
        DereferenceExpression right = (DereferenceExpression) comparison.getRight();
        Identifier leftBase = (Identifier) left.getBase();
        Identifier leftField = left.getField();
        Identifier rightBase = (Identifier) right.getBase();
        Identifier rightField = right.getField();
        assertEquals(f1base, leftBase.getValue());
        assertEquals(f1field, leftField.getValue());
        assertEquals(f2base, rightBase.getValue());
        assertEquals(f2field, rightField.getValue());
    }

    private void assertEqualsFieldString(
            Expression expression, String f1base, String f1field, String literal) {
        ComparisonExpression comparison = (ComparisonExpression) expression;
        assertEquals(
                io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL,
                comparison.getOperator());
        DereferenceExpression left = (DereferenceExpression) comparison.getLeft();
        StringLiteral right = (StringLiteral) comparison.getRight();
        Identifier leftBase = (Identifier) left.getBase();
        Identifier leftField = left.getField();
        assertEquals(f1base, leftBase.getValue());
        assertEquals(f1field, leftField.getValue());
        assertEquals(right.getValue(), literal);
    }

    private void assertEqualsFieldDecimal(
            Expression expression, String f1base, String f1field, String literal) {
        ComparisonExpression comparison = (ComparisonExpression) expression;
        assertEquals(
                io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL,
                comparison.getOperator());
        DereferenceExpression left = (DereferenceExpression) comparison.getLeft();
        DecimalLiteral right = (DecimalLiteral) comparison.getRight();
        Identifier leftBase = (Identifier) left.getBase();
        Identifier leftField = left.getField();
        assertEquals(f1base, leftBase.getValue());
        assertEquals(f1field, leftField.getValue());
        assertEquals(right.getValue(), literal);
    }

    private Query deserializeFromFile(String file) throws IOException {
        InputStream jsonStream = this.getClass().getResourceAsStream(file);
        assertThat(jsonStream, notNullValue());
        return SearchQueryHelper.deserialize(jsonStream);
    }
}
