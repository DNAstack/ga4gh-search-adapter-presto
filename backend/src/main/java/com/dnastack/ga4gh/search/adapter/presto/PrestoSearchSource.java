package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.Table;
import com.dnastack.ga4gh.search.adapter.model.Type;
import com.dnastack.ga4gh.search.adapter.model.request.SearchRequest;
import com.dnastack.ga4gh.search.adapter.model.result.ResultRow;
import com.dnastack.ga4gh.search.adapter.model.result.ResultValue;
import com.dnastack.ga4gh.search.adapter.model.source.SearchSource;
import com.dnastack.ga4gh.search.adapter.presto.PrestoMetadata.PrestoMetadataBuilder;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Query;
import lombok.extern.slf4j.Slf4j;
import org.ga4gh.dataset.SchemaManager;
import org.ga4gh.dataset.model.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class PrestoSearchSource implements SearchSource {

    private final PrestoAdapter prestoAdapter;
    private final Metadata metadata;
    private final SchemaManager schemaManager;
    private final static String RESULT_SET_RELATIVE_URL = "/api/dsresults/%s";


    private final PagingResultSetConsumerCache consumerCache;

    public PrestoSearchSource(PrestoAdapter prestoAdapter) {
        consumerCache = null;
        log.trace("Building presto search source");
        this.prestoAdapter = prestoAdapter;
        this.metadata = new Metadata(buildPrestoMetadata(prestoAdapter));
        log.trace("Registering schemas");
        this.schemaManager = new SchemaManager(false);
        log.trace("Done registering schemas");
    }
    public PrestoSearchSource(PrestoAdapter prestoAdapter, PagingResultSetConsumerCache consumerCache) {
        this.consumerCache = consumerCache;
        log.trace("Building presto search source");
        this.prestoAdapter = prestoAdapter;
        this.metadata = new Metadata(buildPrestoMetadata(prestoAdapter));
        log.trace("Registering schemas");
        //TODO: should be configurable !!
        this.schemaManager = new SchemaManager(false);
        schemaManager.registerSchema("ga4gh_dataset_sample.datasets.subjects", URI.create("https://dnastack.github.io/pgp-canada-schema/Subject.json"));
        log.trace("Done registering schemas");
    }

    public boolean hasConsumerCache() {
        return consumerCache != null;
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
    public Dataset getDataset(String id, Integer pageSize) {
        if (!metadata.hasTable(id)) {
            return null;
        }
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.setSqlQuery(buildDatasetQuery(id));
        Map<String, Object> expectedSchema = schemaManager.getSchema(id);
        searchRequest.setExpectedSchema(expectedSchema);
        return search(searchRequest, pageSize);
    }


    private String buildDatasetQuery(String id) {

        StringBuilder builder = new StringBuilder("SELECT * FROM");
        builder.append(" ").append(id).append(" ");
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
                .description(schema.getOrDefault("description", "No Description").toString())
                .schema(schema)
                .build();
            info.add(di);
        }
        return new ListDatasetsResponse(info);
    }


    @Override
    public Dataset search(SearchRequest searchRequest, Integer pageSize) {
        String prestoSqlString = searchRequest.getSqlQuery();
        log.info("Received SQL: {}", prestoSqlString);
        PageResult pageResult = performPrestoSearchQuery(prestoSqlString, pageSize);
        return createDataset(pageResult, searchRequest.getExpectedSchema());
    }


    @Override
    public Dataset getPaginatedResponse(String token) {
        if (hasConsumerCache()) {
            try {

                String decodedToken = new String(Base64.getDecoder().decode(token));
                String[] tokenParts = decodedToken.split(":");
                if (tokenParts.length != 2) {
                    throw new IllegalArgumentException("Invalid token");
                }
                PagingResultSetConsumer consumer = consumerCache.get(tokenParts[0]);
                return createDataset(consumer.nextPage(tokenParts[1]), null);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new InvalidCacheEntry("Could not retreive paginated response, caching result sets is not configured");
        }
    }

    private PageResult performPrestoSearchQuery(String prestoSqlString, Integer pageSize) {
        try {

            PagingResultSetConsumer consumer = prestoAdapter.query(prestoSqlString, pageSize);
            if (hasConsumerCache()) {
                consumerCache.add(consumer);
            }
            return consumer.firtsPage();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String formPagedResultString(PageResult result) {
        String id = result.getConsumerId() + ":" + result.getNextPageToken();
        return Base64.getEncoder().encodeToString(id.getBytes());
    }

    private Dataset createDataset(PageResult pageResult, Map<String, Object> expectedSchema) {
        boolean hasNextPage = pageResult.getNextPageToken() != null;
        List<Map<String, Object>> results = new ArrayList<>(pageResult.getResults().size());
        for (ResultRow row : pageResult.getResults()) {
            Map<String, Object> rowData = new LinkedHashMap<>();
            for (ResultValue value : row.getValues()) {
                rowData.put(value.getField().getName(), value.getValue());
            }
            results.add(rowData);
        }

        URI nextPageUri = null;
        if (hasNextPage) {
            nextPageUri = ServletUriComponentsBuilder.fromCurrentContextPath()
                .path(String.format(RESULT_SET_RELATIVE_URL, formPagedResultString(pageResult)))
                .build().toUri();
        }
        Pagination pagination = new Pagination(null, nextPageUri);
        if (expectedSchema != null) {
            return new Dataset(expectedSchema, Collections.unmodifiableList(results), pagination);
        }

        Map<String, Object> generatedSchema = generateSchema(pageResult.getFields());
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
        String sqlTemplate =
            "select table_name, table_schema from \"%s\".information_schema.tables";
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("table_name", "Table Name", Type.STRING, null, null, null));
        fields.add(new Field("table_schema", "Table Schema", Type.STRING, null, null, null));

        for (PrestoCatalog catalog : catalogs) {
            String sql = String.format(sqlTemplate, catalog.getName());
            List<ResultRow> catalogTables = query(sql, fields);
            for (ResultRow table : catalogTables) {
                String tableSchema = table.getValues().get(1).getValue().toString();
                if (catalog.getSchema().stream().anyMatch(schema -> schema.getName().equals(tableSchema))) {
                    String tableName = table.getValues().get(0).getValue().toString();
                    String id = String.format("%s.%s.%s", catalog.getName(), tableSchema, tableName);
                    tables.put(id, new PrestoTable(tableName, tableSchema, catalog.getName(), tableSchema, tableName));
                }
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
        List<ResultRow> results;
        try {
            results = query(sql, fields);
        } catch (RuntimeException e) {
            if (e.getCause().getClass() == SQLException.class) {
                log.error("Skipping catalog {} due to errors querying it.", catalogName);
                return false;
            }
            else {
                throw e;
            }
        }

        HashSet<String> blacklist = new HashSet<>();
        Collections.addAll(blacklist, "information", "information_schema", "billing", "connector_views",
                "roles", "databasechangelog", "databasechangeloglock", "billing");
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
        PagingResultSetConsumer consumer = prestoAdapter.query(sql);
        consumer.consumeAll(rs -> {
            try {
                while (rs.next()) {
                    results.add(PagingResultSetConsumer.extractRow(rs, fields));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return results;
    }

    private List<ResultRow> query(String sql, List<Object> params, List<Field> fields) {
        List<ResultRow> results = new ArrayList<>();
        PagingResultSetConsumer consumer = prestoAdapter.query(sql, params);
        consumer.consumeAll(rs -> {
            try {
                while (rs.next()) {
                    results.add(PagingResultSetConsumer.extractRow(rs, fields));
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


}
