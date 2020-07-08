package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.shared.CapturedSearchException;
import com.dnastack.ga4gh.search.adapter.shared.ErrorUtils;
import com.dnastack.ga4gh.search.tables.ListTables;
import com.dnastack.ga4gh.search.tables.Pagination;
import com.dnastack.ga4gh.search.tables.TableData;
import com.dnastack.ga4gh.search.tables.TableError;
import com.dnastack.ga4gh.search.tables.TableInfo;
import com.fasterxml.jackson.databind.JsonNode;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@Slf4j
@Service
public class SearchAdapter {

    private final static String NEXT_PAGE_PATH_TEMPLATE = "/search/%s"; //todo: alternatives?
    private final PrestoClient client;

    @Autowired
    public SearchAdapter(PrestoClient prestoClient) {
        this.client = prestoClient;
    }

    public Single<TableData> search(HttpServletRequest request, String statement, Map<String, String> extraCredentials) {
        long starTime = System.currentTimeMillis();
        return client.query(statement, extraCredentials).map(data -> toTableData(request, data))
            .onErrorReturn(throwable -> {
                throw new CapturedSearchException("search", statement, throwable.getMessage(),
                    System.currentTimeMillis() - starTime, throwable);
            });
    }

    public Single<TableData> getNextPage(HttpServletRequest request, String page, Map<String, String> extraCredentials) {
        long starTime = System.currentTimeMillis();
        return client.next(page, extraCredentials).map(data -> toTableData(request, data))
            .onErrorReturn(throwable -> {
                throw new CapturedSearchException("search", null, throwable.getMessage(),
                    System.currentTimeMillis() - starTime, throwable);
            });
    }

    public Single<ListTables> getTables(HttpServletRequest request, String refHost, Map<String, String> extraCredentials) {
        return getPrestoCatalogs(request, extraCredentials)
            .flatMapObservable(Observable::fromIterable)
            .flatMapSingle(catalog -> {
                String statement = "SELECT table_catalog, table_schema, table_name" +
                    " FROM " + quote(catalog) + ".information_schema.tables" +
                    " WHERE table_schema != 'information_schema'" +
                    " ORDER BY 1, 2, 3";

                return search(request, statement, extraCredentials)
                    .map(tableData -> {
                        log.debug("Reading tables of catalog {} on thread id {}", catalog, Thread.currentThread()
                            .getId());
                        List<TableInfo> tableInfos = new ArrayList<>();
                        for (Map<String, Object> row : tableData.getData()) {
                            String schema = (String) row.get("table_schema");
                            String table = (String) row.get("table_name");
                            String qualifiedTableName = catalog + "." + schema + "." + table;
                            String ref = String.format("%s/table/%s/info", refHost, qualifiedTableName);
                            Map<String, Object> dataModel = new HashMap<>();
                            dataModel.put("$ref", ref);
                            tableInfos.add(new TableInfo(qualifiedTableName, null, dataModel));
                        }
                        return new ListTables(tableInfos, null, null);
                    })
                    .onErrorReturn(throwable -> new ListTables(null, ErrorUtils.handleErrors(throwable), null));
            }).reduce(new ListTables(), (identity, reduction) -> {

                List<TableInfo> infos = reduction.getTableInfos();
                if (infos != null) {
                    if (identity.getTableInfos() == null) {
                        identity.setTableInfos(new ArrayList<>());
                    }
                    identity.getTableInfos().addAll(infos);
                }

                List<TableError> errors = reduction.getErrors();
                if (errors != null) {
                    if (identity.getErrors() == null) {
                        identity.setErrors(new ArrayList<>());
                    }
                    identity.getErrors().addAll(errors);
                }
                return identity;
            });
    }

    private static String quote(String sqlIdentifier) {
        return "\"" + sqlIdentifier.replace("\"", "\"\"") + "\"";
    }

    public Single<TableData> getTableData(HttpServletRequest request, String tableName, String refHost, Map<String, String> extraCredentials) {
        return search(request, "SELECT * FROM " + tableName, extraCredentials)
            .map(data -> {
                data.getDataModel()
                    .put("$id", String.format("%s/table/%s/info", refHost, tableName)); //todo: this could be better
                return data;
            })
            .onErrorReturn(throwable -> {
                if (throwable instanceof CapturedSearchException) {
                    ((CapturedSearchException) throwable).setSource(tableName);

                }
                throw throwable;
            });
    }

    public Single<TableInfo> getTableInfo(HttpServletRequest request, String tableName, String refHost, Map<String, String> extraCredentials) {
        return search(request, "SELECT * FROM " + tableName + " LIMIT 1", extraCredentials)
            .map(data -> {
                data.getDataModel().put("$id", String.format("%s/table/%s/info", refHost, tableName));
                return new TableInfo(tableName, null, data.getDataModel());
            })
            .onErrorReturn(throwable -> {
                if (throwable instanceof CapturedSearchException) {
                    ((CapturedSearchException) throwable).setSource(tableName);

                }
                throw throwable;
            });
    }


    private TableData toTableData(HttpServletRequest request, JsonNode prestoResponse) {

        JsonNode columns;
        Map<String, Object> generatedSchema = new LinkedHashMap<>();
        List<Map<String, Object>> data = new ArrayList<>();

        if (prestoResponse.hasNonNull("columns")) {
            // Generate data model
            columns = prestoResponse.get("columns");
            generatedSchema = generateDataModel(columns);

            // Generate data
            if (prestoResponse.hasNonNull("data")) {
                for (JsonNode dataNode : prestoResponse.get("data")) {
                    Map<String, Object> rowData = new LinkedHashMap<>();
                    for (int i = 0; i < dataNode.size(); i++) {
                        rowData.put(columns.get(i).get("name").asText(), dataNode.get(i).asText());
                    }
                    data.add(rowData);
                }
            }
        }

        // Generate pagination
        Pagination pagination = generatePagination(request, prestoResponse);

        return new TableData(generatedSchema, Collections.unmodifiableList(data), null, pagination);
    }

    private Pagination generatePagination(HttpServletRequest request, JsonNode prestoResponse) {
        URI nextPageUri = null;
        if (prestoResponse.hasNonNull("nextUri")) {
            nextPageUri = ServletUriComponentsBuilder.fromContextPath(request)
                .path(String
                    .format(NEXT_PAGE_PATH_TEMPLATE, URI.create(prestoResponse.get("nextUri").asText()).getPath()))
                .build().toUri();
        }
        return new Pagination(nextPageUri, null);
    }

    private Map<String, Object> generateDataModel(JsonNode columns) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("$id", null);
        schema.put("description", "Automatically generated schema.");
        schema.put("$schema", "http://json-schema.org/draft-07/schema#");
        schema.put("properties", generateJsonSchema(columns));
        return schema;
    }

    private Map<String, Object> generateJsonSchema(JsonNode columns) {
        Map<String, Object> schemaJson = new LinkedHashMap<>();
        int position = 0;
        for (JsonNode column : columns) {
            Map<String, Object> props = new LinkedHashMap<>();
            String type = column.get("type").asText();
            if (JsonAdapter.isArray(type)) {
                props.put("type", "array");
                props.put("items", Map.of("type", JsonAdapter.toJsonType(type)));
            } else {
                props.put("type", JsonAdapter.toJsonType(type));
            }

            props.put("x-ga4gh-position", position++);
            schemaJson.put(column.get("name").asText(), props);
        }

        return schemaJson;
    }

    /**
     * Get a list of the catalogs served by the connected instance of PrestoSQL.
     *
     * @return A List of Strings, where each String is the name of the catalog.
     * @throws IOException If the query to enumerate the list of catalogs fails.
     */
    private Single<List<String>> getPrestoCatalogs(HttpServletRequest request, Map<String, String> extraCredentials) {
        return search(request, "show catalogs", extraCredentials)
            .map(showCatalogs -> {
                List<String> catalogs = new ArrayList<>();
                for (Map<String, Object> row : showCatalogs.getData()) {
                    String catalog = (String) row.get("Catalog");
                    if (catalog.equalsIgnoreCase("system")) {
                        log.debug("Ignoring catalog {}", catalog);
                        continue;
                    }
                    log.debug("Found catalog {}", catalog);
                    catalogs.add(catalog);
                }
                return catalogs;
            });
    }
}
