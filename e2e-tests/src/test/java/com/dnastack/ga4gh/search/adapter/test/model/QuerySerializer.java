package com.dnastack.ga4gh.search.adapter.test.model;

import static com.google.common.base.Preconditions.checkArgument;
import static com.dnastack.ga4gh.search.adapter.test.model.SearchQueryHelper.joinTreeToList;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.ComparisonExpression.Operator;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.Table;

public class QuerySerializer extends JsonSerializer<Query> {

    private static final SearchQueryFieldSerializer SELECT_FIELD_SER =
            new SearchQueryFieldSerializer();
    private static final SearchQueryTableSerializer FROM_FIELD_SER =
            new SearchQueryTableSerializer();
    private static final PredicateSerializer PREDICATE_SER = new PredicateSerializer();

    @Override
    public void serialize(Query value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
        gen.writeStartObject();
        checkArgument(
                value.getQueryBody() instanceof QuerySpecification,
                "Query body must by query spec");
        QuerySpecification querySpec = (QuerySpecification) value.getQueryBody();

        if (querySpec.getSelect() != null) {
            gen.writeFieldName("select");
            gen.writeStartArray();
            for (SelectItem field : querySpec.getSelect().getSelectItems()) {
                SELECT_FIELD_SER.serialize(field, gen, serializers);
            }
            gen.writeEndArray();
        }
        if (querySpec.getFrom().isPresent()) {
            gen.writeFieldName("from");
            gen.writeStartArray();
            Relation from = querySpec.getFrom().get();
            for (Relation relation : joinTreeToList(from)) {
                FROM_FIELD_SER.serialize(relation, gen, serializers);
            }
            gen.writeEndArray();
        }

        if (querySpec.getWhere().isPresent()) {
            gen.writeFieldName("where");
            PREDICATE_SER.serialize(querySpec.getWhere().get(), gen, serializers);
        }

        if (querySpec.getLimit().isPresent()) {
            Limit limit = (Limit) querySpec.getLimit().get();
            gen.writeFieldName("limit");
            gen.writeNumber(limit.getLimit());
        }

        gen.writeEndObject();
    }

    static class SearchQueryFieldSerializer extends JsonSerializer<SelectItem> {
        @Override
        public void serialize(SelectItem value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            checkArgument(value instanceof SingleColumn, "wildcards not supported");
            SingleColumn column = (SingleColumn) value;
            gen.writeStartObject();
            gen.writeFieldName("field");
            gen.writeString(fieldToString(column.getExpression()));
            if (column.getAlias().isPresent()) {
                gen.writeFieldName("alias");
                gen.writeString(column.getAlias().get().getValue());
            }
            gen.writeEndObject();
        }
    }

    static class SearchQueryTableSerializer extends JsonSerializer<Relation> {
        @Override
        public void serialize(Relation value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("table");
            gen.writeString(tableName(value));
            Optional<String> alias = alias(value);
            if (alias.isPresent()) {
                gen.writeFieldName("alias");
                gen.writeString(alias.get());
            }
            gen.writeEndObject();
        }

        private String tableName(Relation value) {
            if (value instanceof Table) {
                Table table = (Table) value;
                checkArgument(table.getName().getParts().size() == 1, "only single part supported");
                return table.getName().getParts().get(0);
            } else if (value instanceof AliasedRelation) {
                AliasedRelation rel = (AliasedRelation) value;
                return tableName(rel.getRelation());
            } else {
                throw new IllegalArgumentException("unsupported relation type");
            }
        }

        private Optional<String> alias(Relation value) {
            if (value instanceof AliasedRelation) {
                AliasedRelation rel = (AliasedRelation) value;
                return Optional.of(rel.getAlias().getValue());
            } else {
                return Optional.empty();
            }
        }
    }

    static class PredicateSerializer extends JsonSerializer<Expression> {
        @Override
        public void serialize(Expression value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("p");
            String key = expressionKey(value);
            gen.writeString(key);
            if (key.equals(ExpressionDeserializer.AND)) {
                List<Expression> predicates = SearchQueryHelper.andTreeToList(value);
                gen.writeFieldName("predicates");
                gen.writeStartArray();
                for (Expression predicate : predicates) {
                    serialize(predicate, gen, serializers);
                }
                gen.writeEndArray();
            } else if (key.equals(ExpressionDeserializer.EQUALS)) {
                ComparisonExpression comparison = (ComparisonExpression) value;
                serializeExpression("l", comparison.getLeft(), gen, serializers);
                serializeExpression("r", comparison.getRight(), gen, serializers);
            }
            gen.writeEndObject();
        }

        private String expressionKey(Expression value) {
            if (value instanceof ComparisonExpression) {
                ComparisonExpression comparison = (ComparisonExpression) value;
                if (comparison.getOperator().equals(Operator.EQUAL)) {
                    return ExpressionDeserializer.EQUALS;
                } else {
                    throw new IllegalArgumentException("unsupported comparison operator");
                }
            } else if (value instanceof LogicalBinaryExpression) {
                LogicalBinaryExpression logExp = (LogicalBinaryExpression) value;
                if (logExp.getOperator()
                        .equals(io.prestosql.sql.tree.LogicalBinaryExpression.Operator.AND)) {
                    return ExpressionDeserializer.AND;
                } else {
                    throw new IllegalArgumentException("unsupported logical operator");
                }
            } else {
                throw new IllegalArgumentException("unsupported expression");
            }
        }

        private void serializeLiteral(String side, Literal value, JsonGenerator gen)
                throws IOException {
            if (value instanceof BooleanLiteral) {
                gen.writeFieldName(side + "boolean");
                gen.writeBoolean(((BooleanLiteral) value).getValue());
            } else if (value instanceof DecimalLiteral) {
                gen.writeFieldName(side + "number");
                gen.writeNumber(((DecimalLiteral) value).getValue());
            } else if (value instanceof StringLiteral) {
                gen.writeFieldName(side + "string");
                gen.writeString(((StringLiteral) value).getValue());
            } else {
                throw new IllegalStateException(
                        "Unsupported type in literal expressions: " + value.getClass());
            }
        }

        private void serializeExpression(
                String side, Expression value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            if (value instanceof Identifier || value instanceof DereferenceExpression) {
                gen.writeFieldName(side + "field");
                gen.writeString(fieldToString(value));
            } else if (value instanceof Literal) {
                serializeLiteral(side, (Literal) value, gen);
            }
        }
    }

    public static String fieldToString(Expression exp) {
        if (exp instanceof Identifier) {
            return ((Identifier) exp).getValue();
        } else if (exp instanceof DereferenceExpression) {
            return toString((DereferenceExpression) exp);
        } else {
            throw new IllegalArgumentException(
                    "unsupported expression for field: " + exp.getClass());
        }
    }

    public static String toString(DereferenceExpression exp) {
        Identifier id = (Identifier) exp.getBase();
        return id.getValue() + "." + exp.getField().getValue();
    }
}
