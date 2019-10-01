package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.Table;
import com.dnastack.ga4gh.search.adapter.model.Type;
import com.dnastack.ga4gh.search.adapter.model.request.DatasetRequest;
import com.dnastack.ga4gh.search.adapter.model.request.SearchRequest;
import com.dnastack.ga4gh.search.adapter.model.result.ResultRow;
import com.dnastack.ga4gh.search.adapter.model.result.ResultValue;
import com.dnastack.ga4gh.search.adapter.model.source.SearchSource;
import com.dnastack.ga4gh.search.adapter.presto.PrestoMetadata.PrestoMetadataBuilder;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Query;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.ga4gh.dataset.SchemaManager;
import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.dataset.model.DatasetInfo;
import org.ga4gh.dataset.model.ListDatasetsResponse;
import org.ga4gh.dataset.model.ListSchemasResponse;
import org.ga4gh.dataset.model.Pagination;
import org.ga4gh.dataset.model.Schema;
import org.springframework.util.Assert;
import org.springframework.web.util.UriComponentsBuilder;

@Slf4j
public class PrestoSearchSource implements SearchSource {

    private final PrestoAdapter prestoAdapter;
    private final Metadata metadata;
    private final SchemaManager schemaManager;
    private final Integer defaultDatasetQuerySize;

    public PrestoSearchSource(PrestoAdapter prestoAdapter, Integer defaultDatasetQuerySize) {
        log.trace("Building presto search source");
        this.prestoAdapter = prestoAdapter;
        this.metadata = new Metadata(buildPrestoMetadata(prestoAdapter));
        log.trace("Registering schemas");
        this.schemaManager = new SchemaManager(false);
        log.trace("Done registering schemas");
        this.defaultDatasetQuerySize = defaultDatasetQuerySize;
    }

    @Override
    public List<Table> getTables() {
        return metadata.getTables();
    }

    //TODO: TOO SLOW
    @Override
    public List<Field> getFields(String tableName) {
        if (tableName == null) {
            return metadata.getFields();
        }
        Table table = metadata.getTable(tableName);
        return metadata.getFields(table);
    }

    @Override
    public Schema getSchema(String id) {
        return null;
    }

    @Override
    public ListSchemasResponse getSchemas() {
        return null;
    }

    @Override
    public Dataset getDataset(DatasetRequest datasetRequest) {
        if (!metadata.hasTable(datasetRequest.getId())) {
            return null;
        }
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setSqlQuery(buildDatasetQuery(datasetRequest));
        Map<String, Object> expectedSchema = schemaManager.getSchema(datasetRequest.getId());
        searchRequest.setExpectedSchema(expectedSchema);
        return search(searchRequest,datasetRequest);
    }


    private String buildDatasetQuery(DatasetRequest datasetRequest) {

        StringBuilder builder = new StringBuilder("SELECT * FROM");
        builder.append(" ").append(datasetRequest.getId()).append(" ");

        if (datasetRequest.getPageSize() == null) {
            datasetRequest.setPageSize(defaultDatasetQuerySize);
        } else {
            Assert.isTrue(datasetRequest.getPageSize() >= 0, "pageSize must be greater or equal to 0");
        }

        if (datasetRequest.getPage() == null) {
            datasetRequest.setPage(1);
        } else {
            Assert.isTrue(datasetRequest.getPageSize() >= 0, "page must be greater or equal to 0");
        }

        builder.append("OFFSET ").append((datasetRequest.getPage() - 1) * datasetRequest.getPageSize()).append(" ");
        builder.append("LIMIT ").append(datasetRequest.getPageSize() + 1);
        return builder.toString();
    }

    private Map<String, Object> generateSchemaProperties(List<Field> fields) {
        Map<String, Object> schemaJson = new LinkedHashMap<>();
        int position = 0;
        for (Field f : fields) {
            Map<String, Object> props = new LinkedHashMap<>();
            props.put("type", f.getType().toString());
            props.put("x-ga4gh-position", position++);
            schemaJson.put(f.getName(), props);
        }
        return schemaJson;
    }


    @Override
    public ListDatasetsResponse getDatasets() {
        List<DatasetInfo> info = new ArrayList<>();
        for (Table t : this.metadata.getTables()) {
            Map<String, Object> schema = schemaManager.getSchema(t.getName());
            if (schema == null) {
                schema = generateSchema(this.metadata.getFields(t));
            }
            DatasetInfo di = DatasetInfo.builder()
                .id(t.getName())
                //TODO: flaky
                .description(schema.get("description").toString())
                .schema(schema)
                .build();
            info.add(di);
        }
        return new ListDatasetsResponse(info);
    }


    @Override
    public Dataset search(SearchRequest searchRequest) {
        String prestoSqlString = searchRequest.getSqlQuery();
        log.info("Received SQL: {}", prestoSqlString);
        List<Field> fields = new ArrayList<>();
        List<ResultRow> results = new ArrayList<>();
        performPrestoSearchQuery(prestoSqlString, fields, results);
        return createDataset(fields, results, searchRequest.getExpectedSchema(), null);
    }

    private Dataset search(SearchRequest searchRequest, DatasetRequest datasetRequest) {
        String prestoSqlString = searchRequest.getSqlQuery();
        log.info("Received SQL: {}", prestoSqlString);
        List<Field> fields = new ArrayList<>();
        List<ResultRow> results = new ArrayList<>();
        performPrestoSearchQuery(prestoSqlString, fields, results);
        return createDataset(fields, results, searchRequest.getExpectedSchema(), datasetRequest);
    }

    private void performPrestoSearchQuery(String prestoSqlString, List<Field> fields, List<ResultRow> results) {
        prestoAdapter.query(
            prestoSqlString,
            rs -> {
                try {
                    ResultSetMetaData resultSetMetaData = rs.getMetaData();
                    fields.addAll(extractFields(resultSetMetaData));
                    while (rs.next()) {
                        results.add(extractRow(rs, fields));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    private Dataset createDataset(List<Field> fields, List<ResultRow> queryResults, Map<String, Object> expectedSchema, DatasetRequest datasetRequest) {
        boolean hasNextPage = datasetRequest != null && datasetRequest.getPageSize() < queryResults.size();
        int resultSize = hasNextPage ? datasetRequest.getPageSize() : queryResults.size();
        List<Map<String, Object>> results = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            ResultRow row = queryResults.get(i);
            Map<String, Object> rowData = new LinkedHashMap<>();
            for (ResultValue value : row.getValues()) {
                rowData.put(value.getField().getName(), value.getValue());
            }
            results.add(rowData);
        }

        URI previousPageUri = null;
        URI nextPageUri = null;
        if (datasetRequest != null) {
            if (datasetRequest.getPage() > 1) {
                previousPageUri = UriComponentsBuilder.fromHttpUrl(datasetRequest.getUrl())
                    .queryParam("page", datasetRequest.getPage() - 1)
                    .queryParam("pageSize", datasetRequest.getPageSize())
                    .build().toUri();
            }
            if (hasNextPage) {
                nextPageUri = UriComponentsBuilder.fromHttpUrl(datasetRequest.getUrl())
                    .queryParam("page", datasetRequest.getPage() + 1)
                    .queryParam("pageSize", datasetRequest.getPageSize())
                    .build().toUri();
            }
        }
        Pagination pagination = new Pagination(previousPageUri, nextPageUri);
        if (expectedSchema != null) {
            return new Dataset(expectedSchema, Collections.unmodifiableList(results), pagination);
        }

        Map<String, Object> generatedSchema = generateSchema(fields);
        return new Dataset(generatedSchema, Collections.unmodifiableList(results), pagination);
    }

    private Map<String, Object> generateSchema(List<Field> fields) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("$id", "GENERATED_SCHEMA"); // Query ID?
        schema.put("description", "Automatically generated schema.");
        schema.put("$ref", null);
        schema.put("$schema", "http://json-schema.org/draft-07/schema#");
        Map<String, Object> properties = generateSchemaProperties(fields);
        schema.put("properties", properties);
        return schema;
    }

    private List<Field> extractFields(ResultSetMetaData resultSetMetaData) throws SQLException {
        List<Field> fields = new ArrayList<>(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            String prestoType = resultSetMetaData.getColumnTypeName(i);
            Type primitiveType = Metadata.prestoToPrimitiveType(prestoType);
            String[] typeOperators = Metadata.operatorsForType(primitiveType);
            //TODO: This data is not populated correctly -- why?
            String qualifiedTableName = String.format("%s.%s.%s",
                resultSetMetaData.getCatalogName(i),
                resultSetMetaData.getSchemaName(i),
                resultSetMetaData.getTableName(i));
            String id = qualifiedTableName + "." + columnName;
            //TODO: Temporary workaround while above is unpopulated
            Field f = new Field(columnName, columnName, primitiveType, typeOperators, null, qualifiedTableName);
            //Field f = new Field(id, columnName, primitiveType, typeOperators, null, qualifiedTableName);
            fields.add(f);
        }

        return fields;
    }

    private PrestoMetadata buildPrestoMetadata(PrestoAdapter adapter) {
        PrestoMetadataBuilder prestoMetadata = PrestoMetadata.builder();
        List<PrestoCatalog> catalogs = getCatalogMetadata();
        Map<String, PrestoTable> tables = getTableMetadata(catalogs);
        Map<PrestoTable, List<PrestoField>> fields = getFieldMetadata(tables);
        prestoMetadata.presto(adapter);
        prestoMetadata.catalogs(catalogs);
        prestoMetadata.tables(tables);
        prestoMetadata.fields(fields);
        return prestoMetadata.build();
    }

    private List<PrestoCatalog> getCatalogMetadata() {
        log.trace("Retrieving catalog metadata");
        List<PrestoCatalog.PrestoCatalogBuilder> emptyCatalogs = getCatalogBuilders();
        List<PrestoCatalog> populatedCatalogs = new ArrayList<>(emptyCatalogs.size());
        for (PrestoCatalog.PrestoCatalogBuilder catalog : emptyCatalogs) {
            if (tryPopulateSchemas(catalog)) {
                populatedCatalogs.add(catalog.build());
            }
        }
        return populatedCatalogs;
    }

    private Map<PrestoTable, List<PrestoField>> getFieldMetadata(Map<String, PrestoTable> tables) {
        List<PrestoTable> t = new ArrayList<>();
        tables.forEach((k, v) -> t.add(v));
        ConcurrentHashMap<PrestoTable, List<PrestoField>> fieldMetadata = new ConcurrentHashMap<>();
        // TODO: This is maybe a temporary workaround, re-evaluate using common pool (or customize?)
        t.parallelStream().forEach(prestoTable -> {
            PrestoTableMetadata metadata = prestoAdapter.getMetadata(prestoTable);
            fieldMetadata.put(prestoTable, metadata.getFields());
        });
        return Collections.unmodifiableMap(fieldMetadata);
    }

    private Map<String, PrestoTable> getTableMetadata(List<PrestoCatalog> catalogs) {
        Map<String, PrestoTable> tables = new HashMap<>();
        //TODO: better table blackisting than hardcoded in SQL query
        //TODO: Consolidate blacklisting, there is a weird overlap in how things are blacklisted at the
        // catalog, schema, and table level.
        String sqlTemplate =
            "select table_name, table_schema from \"%s\".information_schema.tables WHERE table_schema NOT IN " +
                "('information_schema', 'connector_views', 'roles', 'databasechangelog', 'databasechangeloglock', 'billing')";
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("table_name", "Table Name", Type.STRING, null, null, null));
        fields.add(new Field("table_schema", "Table Schema", Type.STRING, null, null, null));

        for (PrestoCatalog catalog : catalogs) {
            String sql = String.format(sqlTemplate, catalog.getName());
            List<ResultRow> catalogTables = query(sql, fields);
            for (ResultRow table : catalogTables) {
                //TODO: Better string manip
                String tableName = table.getValues().get(0).getValue().toString();
                String tableSchema = table.getValues().get(1).getValue().toString();
                String id = String.format("%s.%s.%s", catalog.getName(), tableSchema, tableName);
                tables.put(id, new PrestoTable(tableName, tableSchema, catalog.getName(), tableSchema, tableName));
            }
        }

        return tables;
    }

    private List<PrestoCatalog.PrestoCatalogBuilder> getCatalogBuilders() {
        log.trace("Retrieving all catalogs and generating builders");
        final String sql = "show catalogs";
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("Catalog", "Catalog", Type.STRING, null, null, null));
        List<ResultRow> results = query(sql, fields);
        List<PrestoCatalog.PrestoCatalogBuilder> catalogBuilders = new ArrayList<>();
        HashSet<String> blacklist = new HashSet<>();
        blacklist.add("system");
        results.forEach(row -> {
            String name = row.getValues().get(0).getValue().toString();
            if (blacklist.contains(name)) {
                return;
            }
            catalogBuilders.add(PrestoCatalog.builder().name(name));
        });

        return catalogBuilders;
    }

    //TODO: better signature
    private boolean tryPopulateSchemas(PrestoCatalog.PrestoCatalogBuilder catalog) {
        String catalogName = catalog.build().getName();
        //TODO: better enforcement of partial build state requirements
        if (catalogName == null) {
            return false;
        }
        log.trace("Attempting to populate schema for catalog: {}", catalog);
        String sql = String.format("SHOW SCHEMAS FROM \"%s\"", catalogName);
        List<Field> fields = Collections.singletonList(new Field("Schema", "Schema", Type.STRING, null, null, null));
        List<ResultRow> results = query(sql, fields);

        HashSet<String> blacklist = new HashSet<>();
        blacklist.add("information");
        blacklist.add("information_schema");
        blacklist.add("billing");
        List<PrestoSchema> schemas = new ArrayList<>();
        for (ResultRow row : results) {
            List<ResultValue> values = row.getValues();
            values.stream()
                .filter(v -> !blacklist.contains(v.getValue().toString()))
                .forEach(v -> schemas.add(new PrestoSchema(v.getValue().toString())));
        }
        catalog.schemas(schemas);
        log.trace("Successfully populated schema for catalog: {}", catalog);
        return true;
    }

    private List<ResultRow> query(String sql, List<Field> fields) {
        List<ResultRow> results = new ArrayList<>();
        prestoAdapter.query(
            sql,
            rs -> {
                try {
                    while (rs.next()) {
                        results.add(extractRow(rs, fields));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        return results;
    }

    private List<ResultRow> query(String sql, List<Object> params, List<Field> fields) {
        List<ResultRow> results = new ArrayList<>();
        prestoAdapter.query(
            sql,
            params,
            rs -> {
                try {
                    while (rs.next()) {
                        results.add(extractRow(rs, fields));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        return results;
    }

    private Query getQuery(SearchRequest query) {
        if (query.getSqlQuery() != null) {
            if (query.getJsonQuery() != null) {
                log.warn("Received both JSON and SQL query, ignoring JSON");
            }
            return parseQuery(query.getSqlQuery());
        } else if (query.getJsonQuery() != null) {
            log.debug("Processing JSON query");
            return query.getJsonQuery();
        } else {
            throw new IllegalArgumentException(
                "Either JSON or SQL query has to be present in search request");
        }
    }

    private Query parseQuery(String sql) {
        log.debug("Processing SQL query: {}", sql);
        return (Query) new SqlParser().createStatement(sql, new ParsingOptions());
    }

    private ResultRow extractRow(ResultSet rs, List<Field> fields) throws SQLException {
        List<ResultValue> values = new ArrayList<>();

        for (int i = 1; i <= fields.size(); i++) {
            values.add(new ResultValue(fields.get(i - 1), rs.getString(i)));
        }

        return new ResultRow(values);
    }
}
