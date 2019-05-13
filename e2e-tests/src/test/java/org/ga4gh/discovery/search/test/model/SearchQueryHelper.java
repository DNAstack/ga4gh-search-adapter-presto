package org.ga4gh.discovery.search.test.model;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.ga4gh.discovery.search.test.model.QueryDeserializer;
import org.ga4gh.discovery.search.test.model.QuerySerializer;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;

import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.ComparisonExpression.Operator;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GroupBy;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.Join.Type;
import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Offset;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.With;

public class SearchQueryHelper {

    public static Query exampleQuery() {
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
                noOffset(),
                limit(10));
    }

    /** Generates the demo_view definition. */
    public static Query demoViewQuery() {
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
                noOffset(),
                noLimit());
    }

    public static Query query(
            Select select,
            Optional<Relation> from,
            Optional<Expression> where,
            Optional<Offset> offset,
            Optional<Node> limit) {
        Optional<With> with = Optional.empty();
        Optional<OrderBy> orderBy = Optional.empty();
        Optional<GroupBy> groupBy = Optional.empty();
        Optional<Expression> having = Optional.empty();
        QuerySpecification spec =
                new QuerySpecification(
                        select, from, where, groupBy, having, orderBy, offset, limit);
        return new Query(with, spec, orderBy, noOffset(), noLimit());
    }

    public static StringLiteral literalStr(String value) {
        return new StringLiteral(value);
    }

    public static DecimalLiteral literalNum(String value) {
        return new DecimalLiteral(value);
    }

    public static Expression fieldRef(String reference) {
        String[] fieldTuple = reference.split("\\.");
        checkArgument(fieldTuple.length < 3, "Wrong field reference format: " + reference);
        if (fieldTuple.length == 1) {
            return new Identifier(fieldTuple[0], true);
        } else {
            return new DereferenceExpression(
                    new Identifier(fieldTuple[0], true), new Identifier(fieldTuple[1], true));
        }
    }

    public static ComparisonExpression equals(
            Expression leftExpression, Expression rightExpression) {
        return new ComparisonExpression(Operator.EQUAL, leftExpression, rightExpression);
    }

    public static Expression and(Expression... expressions) {
        return andListToTree(Arrays.asList(expressions));
    }

    public static SingleColumn fieldAs(String fieldRef, String alias) {
        return new SingleColumn(fieldRef(fieldRef), Optional.of(new Identifier(alias, true)));
    }

    public static SingleColumn field(String fieldRef) {
        return new SingleColumn(fieldRef(fieldRef), Optional.empty());
    }

    public static Table table(String tableName) {
        return new Table(QualifiedName.of(tableName));
    }

    public static AliasedRelation tableAs(String tableName, String alias) {
        return new AliasedRelation(
                table(tableName), new Identifier(alias, true), ImmutableList.of());
    }

    public static Select select(SelectItem... fields) {
        return new Select(false, Arrays.asList(fields));
    }

    public static Optional<Relation> from(Relation... tables) {
        return Optional.of(joinListToTree(Arrays.asList(tables)));
    }

    public static Optional<Expression> where(Expression predicate) {
        return Optional.of(predicate);
    }

    public static Optional<Expression> noWhere() {
        return Optional.empty();
    }

    public static Optional<Node> limit(int limit) {
        return Optional.of(new Limit(Integer.toString(limit)));
    }

    public static Optional<Node> noLimit() {
        return Optional.empty();
    }

    public static Optional<Offset> noOffset() {
        return Optional.empty();
    }

    public static Optional<Offset> offset(int offset) {
        return Optional.of(new Offset(Integer.toString(offset)));
    }

    public static ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();

        SimpleModule module =
                new SimpleModule("TestSerialization", new Version(1, 0, 0, null, null, null));
        module.addDeserializer(Query.class, new QueryDeserializer());
        module.addSerializer(Query.class, new QuerySerializer());
        objectMapper.registerModule(module);

        return objectMapper;
    }

    public static Query deserialize(InputStream jsonStream) throws IOException {
        return objectMapper().readValue(jsonStream, Query.class);
    }

    public static String serializeToString(Query query) throws IOException {
        return objectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(query);
    }

    public static Relation joinListToTree(List<Relation> relations) {
        checkArgument(!relations.isEmpty(), "there must be some relations");
        if (relations.size() == 1) {
            return relations.get(0);
        } else {
            Relation lastRelation = relations.get(relations.size() - 1);
            List<Relation> rest = relations.subList(0, relations.size() - 1);
            return new Join(Type.IMPLICIT, joinListToTree(rest), lastRelation, Optional.empty());
        }
    }

    public static List<Relation> joinTreeToList(Relation root) {
        if (root instanceof AliasedRelation || root instanceof Table) {
            return ImmutableList.of(root);
        } else if (root instanceof Join) {
            Join join = (Join) root;
            return ImmutableList.<Relation>builder()
                    .addAll(joinTreeToList(join.getLeft()))
                    .addAll(joinTreeToList(join.getRight()))
                    .build();
        } else {
            throw new IllegalArgumentException("only joins and relations supported");
        }
    }

    public static Expression andListToTree(List<Expression> expressions) {
        checkArgument(!expressions.isEmpty(), "AND predicate has to have expressions");
        if (expressions.size() == 1) {
            return expressions.get(0);
        }
        Expression last = expressions.get(expressions.size() - 1);
        List<Expression> rest = expressions.subList(0, expressions.size() - 1);
        return new LogicalBinaryExpression(
                io.prestosql.sql.tree.LogicalBinaryExpression.Operator.AND,
                andListToTree(rest),
                last);
    }

    public static List<Expression> andTreeToList(Expression root) {
        if (root instanceof ComparisonExpression) {
            return ImmutableList.of(root);
        } else if (root instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression logExpr = (LogicalBinaryExpression) root;
            if (io.prestosql.sql.tree.LogicalBinaryExpression.Operator.AND.equals(
                    logExpr.getOperator())) {
                return ImmutableList.<Expression>builder()
                        .addAll(andTreeToList(logExpr.getLeft()))
                        .addAll(andTreeToList(logExpr.getRight()))
                        .build();
            } else {
                throw new IllegalArgumentException("only AND operators allowed");
            }
        } else {
            throw new IllegalArgumentException("only AND and comparison expressions supported");
        }
    }
}
