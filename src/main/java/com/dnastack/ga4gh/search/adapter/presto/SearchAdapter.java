package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.shared.AuthRequiredException;
import com.dnastack.ga4gh.search.adapter.shared.SearchAuthRequest;
import com.dnastack.ga4gh.search.tables.ListTables;
import com.dnastack.ga4gh.search.tables.Pagination;
import com.dnastack.ga4gh.search.tables.TableData;
import com.dnastack.ga4gh.search.tables.TableError;
import com.dnastack.ga4gh.search.tables.TableError.ErrorCode;
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
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@Slf4j
public class SearchAdapter {

    private final static String NEXT_PAGE_PATH_TEMPLATE = "/search/%s"; //todo: alternatives?
    private final PrestoClient client;
    private final Map<String, String> extraCredentials;

    public SearchAdapter(PrestoClient prestoClient, Map<String, String> extraCredentials) {
        this.client = prestoClient;
        this.extraCredentials = extraCredentials;
    }

    public Single<TableData> search(String statement) {
        return client.query(statement, extraCredentials)
            .map(this::toTableData);
    }

    public Single<TableData> getNextPage(String page) {
        return client.next(page, extraCredentials).map(this::toTableData);
    }

    public Single<ListTables> getTables(String refHost) {
        return getPrestoCatalogs()
            .flatMapObservable(Observable::fromIterable)
            .flatMapSingle(catalog -> {
                String statement =
                    "SELECT table_catalog, table_schema, table_name" +
                        " FROM " + quote(catalog) + ".information_schema.tables" +
                        " WHERE table_schema != 'information_schema'" +
                        " ORDER BY 1, 2, 3";
                return search(statement)
                    .map(tableData -> {
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
                    .onErrorReturn(throwable -> {
                        if (throwable instanceof AuthRequiredException) {
                            SearchAuthRequest searchAuthRequest = ((AuthRequiredException) throwable)
                                .getAuthorizationRequest();
                            TableError error = new TableError();
                            error.setMessage("User is not authorized to access catalog: " + searchAuthRequest.getKey()
                                + ", request requires additional authorization information");
                            error.setSource(searchAuthRequest.getKey());
                            error.setCode(ErrorCode.AUTH_CHALLENGE);
                            error.setAttributes(searchAuthRequest.getResourceDescription());
                            return new ListTables(null, List.of(error), null);
                        } else if (throwable instanceof TimeoutException) {
                            TableError error = new TableError();
                            error.setMessage(
                                "Request to catalog: " + catalog + " timedout before a response was committed.");
                            error.setSource(catalog);
                            error.setCode(ErrorCode.TIMEOUT);
                            return new ListTables(null, List.of(error), null);
                        } else {
                            throw throwable;
                        }

                    });
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

    public Single<TableData> getTableData(String tableName, String refHost)  {
        return search("SELECT * FROM " + tableName)
            .map(data -> {
                data.getDataModel()
                    .put("$id", String.format("%s/table/%s/info", refHost, tableName)); //todo: this could be better
                return data;
            });
    }

    public Single<TableInfo> getTableInfo(String tableName, String refHost) {
        return search("SELECT * FROM " + tableName + " LIMIT 1")
            .map(data -> {
                data.getDataModel().put("$id", String.format("%s/table/%s/info", refHost, tableName));
                return new TableInfo(tableName, null, data.getDataModel());
            });
    }

    private TableData toTableData(JsonNode prestoResponse) {

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
        Pagination pagination = generatePagination(prestoResponse);

        return new TableData(generatedSchema, Collections.unmodifiableList(data), pagination);
    }

    private Pagination generatePagination(JsonNode prestoResponse) {
        URI nextPageUri = null;
        if (prestoResponse.hasNonNull("nextUri")) {
            nextPageUri = ServletUriComponentsBuilder.fromCurrentContextPath()
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
    private Single<List<String>> getPrestoCatalogs(){
        return search("show catalogs")
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
