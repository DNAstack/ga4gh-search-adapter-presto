package org.ga4gh.discovery.search.model.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.StringLiteral;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.ga4gh.discovery.search.model.serde.ExpressionDeserializer;
import org.ga4gh.discovery.search.model.serde.QuerySerializer;
import org.junit.Test;

public class ExpressionDeserializerTest {

    @Test
    public void testDeserializer() throws IOException {
        Expression predicate = deserializeFromFile("/predicate_complex_join.json");
        assertThat(predicate, instanceOf(LogicalBinaryExpression.class));
        List<Expression> predicates = SearchQueryHelper.andTreeToList(predicate);
        assertThat(predicates, hasSize(12));

        for (Expression subPredicate : predicates) {
            assertThat(subPredicate, instanceOf(ComparisonExpression.class));
        }

        ComparisonExpression eq1 = (ComparisonExpression) predicates.get(0);
        ComparisonExpression eq4 = (ComparisonExpression) predicates.get(3);
        ComparisonExpression eq5 = (ComparisonExpression) predicates.get(4);
        ComparisonExpression eq12 = (ComparisonExpression) predicates.get(11);

        assertExprFieldField(eq1, "f_drs.participant_id", "fac.participant_id");
        assertExprFieldField(eq4, "drs.id", "f_drs.raw_value");

        assertExprFieldValue(eq5, "f_drs.key", "Source VCF object ID");
        assertExprFieldValue(eq12, "var.alternate_base", "G");
    }

    private void assertExprFieldField(
            ComparisonExpression eq, String leftField, String rightField) {
        assertThat(eq.getLeft(), instanceOf(DereferenceExpression.class));
        assertThat(eq.getRight(), instanceOf(DereferenceExpression.class));
        DereferenceExpression leftFieldReference = (DereferenceExpression) eq.getLeft();
        DereferenceExpression rightFieldReference = (DereferenceExpression) eq.getRight();
        assertThat(QuerySerializer.toString(leftFieldReference), is(leftField));
        assertThat(QuerySerializer.toString(rightFieldReference), is(rightField));
    }

    private void assertExprFieldValue(
            ComparisonExpression eq, String leftField, String rightValue) {
        assertThat(eq.getLeft(), instanceOf(DereferenceExpression.class));
        assertThat(eq.getRight(), instanceOf(StringLiteral.class));
        DereferenceExpression leftFieldReference = (DereferenceExpression) eq.getLeft();
        StringLiteral rightLiteralExpression = (StringLiteral) eq.getRight();
        assertThat(QuerySerializer.toString(leftFieldReference), is(leftField));
        assertThat(rightLiteralExpression.getValue(), is(rightValue));
    }

    private Expression deserializeFromFile(String file) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        InputStream jsonStream = this.getClass().getResourceAsStream(file);
        assertThat(jsonStream, notNullValue());

        SimpleModule module =
                new SimpleModule("TestSerialization", new Version(1, 0, 0, null, null, null));
        module.addDeserializer(Expression.class, new ExpressionDeserializer());
        objectMapper.registerModule(module);
        return objectMapper.readValue(jsonStream, Expression.class);
    }
}
