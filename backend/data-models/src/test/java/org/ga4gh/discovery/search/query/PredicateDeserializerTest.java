package org.ga4gh.discovery.search.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.ga4gh.discovery.search.serde.PredicateDeserializer;
import org.junit.Test;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class PredicateDeserializerTest {

    @Test
    public void testDeserializer() throws IOException {
        Predicate predicate = deserializeFromFile("/predicate_complex_join.json");
        assertThat(predicate.getKey(), is("and"));
        And and = (And) predicate;
        List<Predicate> predicates = and.getPredicates();
        assertThat(predicates, hasSize(12));

        for (Predicate subPredicate : predicates) {
            assertThat(subPredicate.getKey(), is("="));
        }

        Equals eq1 = (Equals) predicates.get(0);
        Equals eq4 = (Equals) predicates.get(3);
        Equals eq5 = (Equals) predicates.get(4);
        Equals eq12 = (Equals) predicates.get(11);

        assertExprFieldField(eq1, "f_drs.participant_id", "fac.participant_id");
        assertExprFieldField(eq4, "drs.id", "f_drs.raw_value");

        assertExprFieldValue(eq5, "f_drs.key", "Source VCF object ID");
        assertExprFieldValue(eq12, "var.alternate_base", "G");
    }

    private void assertExprFieldField(Equals eq, String leftField, String rightField) {
        assertThat(eq.getLeftExpression(), instanceOf(FieldReference.class));
        assertThat(eq.getRightExpression(), instanceOf(FieldReference.class));
        FieldReference leftFieldReference = (FieldReference) eq.getLeftExpression();
        FieldReference rightFieldReference = (FieldReference) eq.getRightExpression();
        assertThat(leftFieldReference.toString(), is(leftField));
        assertThat(rightFieldReference.toString(), is(rightField));
    }

    private void assertExprFieldValue(Equals eq, String leftField, String rightValue) {
        assertThat(eq.getLeftExpression(), instanceOf(FieldReference.class));
        assertThat(eq.getRightExpression(), instanceOf(LiteralExpression.class));
        FieldReference leftFieldReference = (FieldReference) eq.getLeftExpression();
        LiteralExpression rightLiteralExpression = (LiteralExpression) eq.getRightExpression();
        assertThat(leftFieldReference.toString(), is(leftField));
        assertThat(rightLiteralExpression.getValue(), is(rightValue));
    }

    private Predicate deserializeFromFile(String file) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        InputStream jsonStream = this.getClass().getResourceAsStream(file);
        assertThat(jsonStream, notNullValue());

        SimpleModule module =
                new SimpleModule("TestSerialization", new Version(1, 0, 0, null, null, null));
        module.addDeserializer(Predicate.class, new PredicateDeserializer());
        objectMapper.registerModule(module);
        return objectMapper.readValue(jsonStream, Predicate.class);
    }
}
