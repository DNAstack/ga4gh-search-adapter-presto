package org.ga4gh.discovery.search.test.model;

import java.io.IOException;
import org.ga4gh.discovery.search.test.model.Type;
import org.ga4gh.discovery.search.test.model.And;
import org.ga4gh.discovery.search.test.model.BinaryOperator;
import org.ga4gh.discovery.search.test.model.Equals;
import org.ga4gh.discovery.search.test.model.Expression;
import org.ga4gh.discovery.search.test.model.FieldReference;
import org.ga4gh.discovery.search.test.model.Like;
import org.ga4gh.discovery.search.test.model.LiteralExpression;
import org.ga4gh.discovery.search.test.model.Predicate;
import org.ga4gh.discovery.search.test.model.SearchQuery;
import org.ga4gh.discovery.search.test.model.SearchQueryField;
import org.ga4gh.discovery.search.test.model.SearchQueryTable;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableSet;

public class SearchQuerySerializer extends JsonSerializer<SearchQuery> {

    private static final SearchQueryFieldSerializer SELECT_FIELD_SER =
            new SearchQueryFieldSerializer();
    private static final SearchQueryTableSerializer FROM_FIELD_SER =
            new SearchQueryTableSerializer();
    private static final PredicateSerializer PREDICATE_SER = new PredicateSerializer();

    @Override
    public void serialize(SearchQuery value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
        gen.writeStartObject();

        if (value.getSelect() != null) {
            gen.writeFieldName("select");
            gen.writeStartArray();
            for (SearchQueryField field : value.getSelect()) {
                SELECT_FIELD_SER.serialize(field, gen, serializers);
            }
            gen.writeEndArray();
        }

        if (value.getFrom() != null) {
            gen.writeFieldName("from");
            gen.writeStartArray();
            for (SearchQueryTable table : value.getFrom()) {
                FROM_FIELD_SER.serialize(table, gen, serializers);
            }
            gen.writeEndArray();
        }

        if (value.getWhere().isPresent()) {
            gen.writeFieldName("where");
            PREDICATE_SER.serialize(value.getWhere().get(), gen, serializers);
        }

        if (value.getLimit().isPresent()) {
            gen.writeFieldName("limit");
            gen.writeNumber(value.getLimit().getAsLong());
        }

        if (value.getOffset().isPresent()) {
            gen.writeFieldName("offset");
            gen.writeNumber(value.getOffset().getAsLong());
        }

        gen.writeEndObject();
    }

    static class SearchQueryFieldSerializer extends JsonSerializer<SearchQueryField> {
        @Override
        public void serialize(
                SearchQueryField value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("field");
            gen.writeString(value.getFieldReference().toString());
            if (value.getAlias().isPresent()) {
                gen.writeFieldName("alias");
                gen.writeString(value.getAlias().get());
            }
            gen.writeEndObject();
        }
    }

    static class SearchQueryTableSerializer extends JsonSerializer<SearchQueryTable> {
        @Override
        public void serialize(
                SearchQueryTable value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("table");
            gen.writeString(value.getTableName());
            if (value.getAlias().isPresent()) {
                gen.writeFieldName("alias");
                gen.writeString(value.getAlias().get());
            }
            gen.writeEndObject();
        }
    }

    static class PredicateSerializer extends JsonSerializer<Predicate> {
        @Override
        public void serialize(Predicate value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("p");
            gen.writeString(value.getKey());
            if (value.getKey().equals(And.KEY)) {
                And and = (And) value;
                if (and.getPredicates() != null) {
                    gen.writeFieldName("predicates");
                    gen.writeStartArray();
                    for (Predicate predicate : and.getPredicates()) {
                        serialize(predicate, gen, serializers);
                    }
                    gen.writeEndArray();
                }
            } else if (ImmutableSet.of(Equals.KEY, Like.KEY).contains(value.getKey())) {
                BinaryOperator binOp = (BinaryOperator) value;
                if (binOp.getLeftExpression() != null) {
                    serializeExpression("l", binOp.getLeftExpression(), gen, serializers);
                }
                if (binOp.getRightExpression() != null) {
                    serializeExpression("r", binOp.getRightExpression(), gen, serializers);
                }
            }
            gen.writeEndObject();
        }

        private void serializeLiteral(String side, Type type, String value, JsonGenerator gen)
                throws IOException {
            switch (type) {
                case BOOLEAN:
                    gen.writeFieldName(side + "boolean");
                    gen.writeBoolean(Boolean.parseBoolean(value));
                    break;
                case NUMBER:
                    gen.writeFieldName(side + "number");
                    gen.writeNumber(value);
                    break;
                case STRING:
                    gen.writeFieldName(side + "string");
                    gen.writeString(value);
                    break;
                default:
                    throw new IllegalStateException(
                            "Unsupported type in literal expressions: " + type);
            }
        }

        private void serializeExpression(
                String side, Expression value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            if (value instanceof FieldReference) {
                FieldReference fieldRef = (FieldReference) value;
                gen.writeFieldName(side + "field");
                gen.writeString(fieldRef.toString());
            } else if (value instanceof LiteralExpression) {
                LiteralExpression exp = (LiteralExpression) value;
                serializeLiteral(side, exp.getType(), exp.getValue(), gen);
            }
        }
    }
}
