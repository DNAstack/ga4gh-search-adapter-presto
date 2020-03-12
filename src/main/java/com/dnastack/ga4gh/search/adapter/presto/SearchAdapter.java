package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.tables.ListTables;
import com.dnastack.ga4gh.search.tables.Pagination;
import com.dnastack.ga4gh.search.tables.TableData;
import com.dnastack.ga4gh.search.tables.TableInfo;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.*;

@Slf4j
public class SearchAdapter {

    private final static String NEXT_PAGE_PATH_TEMPLATE = "/search/%s"; //todo: alternatives?
    private final PrestoClient client;

    public SearchAdapter(PrestoClient prestoClient) {
        this.client = prestoClient;
    }

    public TableData search(String statement) {
        JsonNode response = client.query(statement);
        return toTableData(response);
    }

    public TableData getNextPage(String page) {
        JsonNode data = client.next(page);
        return toTableData(data);
    }

    public ListTables getTables(String refHost) {
        ListTables listTables = new ListTables();
        List<String> schemas = getPrestoSchemas();
        List<TableInfo> tableInfos = new ArrayList<>();
        for (String schema : schemas) {
            String statement = "SHOW TABLES FROM " + schema;
            TableData tableData = search(statement);
            if (tableData == null) { //TODO: better way of handling this sitaution
                log.warn("Schema " + schema + "'s tables were not retrievable!");
                continue;
            }
            for (Map<String, Object> data : tableData.getData()) {
                String table = data.get("Table").toString();
                String qualifiedTableName = schema + "." + table;
                String ref = String.format("%s/table/%s/info", refHost, qualifiedTableName);
                Map<String, Object> dataModel = new HashMap<>();
                dataModel.put("$ref", ref);
                tableInfos.add(new TableInfo(qualifiedTableName, null, dataModel));
            }
        }

        listTables.setTableInfos(tableInfos);
        return listTables;
    }

    public TableData getTableData(String tableName, String refHost) {
        TableData data = search("SELECT * FROM " + tableName);
        data.getDataModel().put("$id", String.format("%s/table/%s/info", refHost, tableName)); //todo: this could be better
        return data;
    }

    public TableInfo getTableInfo(String tableName, String refHost) {
        TableData data = search("SELECT * FROM " + tableName + " LIMIT 1");
        data.getDataModel().put("$id", String.format("%s/table/%s/info", refHost, tableName));
        return new TableInfo(tableName, null, data.getDataModel());
    }

    private TableData toTableData(JsonNode prestoResponse) {
        if (prestoResponse == null) {
            return null;
        }

        JsonNode columns = prestoResponse.get("columns");
        Map<String, Object> generatedSchema = generateDataModel(columns);
        List<Map<String, Object>> data = new ArrayList<>();
        if (prestoResponse.hasNonNull("data")) {
            for (JsonNode dataNode : prestoResponse.get("data")) {
                Map<String, Object> rowData = new LinkedHashMap<>();
                for (int i = 0; i < dataNode.size(); i++) {
                    rowData.put(columns.get(i).get("name").asText(), dataNode.get(i).asText());
                }
                data.add(rowData);
            }
        }

        Pagination pagination = generatePagination(prestoResponse);
        return new TableData(generatedSchema, Collections.unmodifiableList(data), pagination);
    }

    private Pagination generatePagination(JsonNode prestoResponse) {
        URI nextPageUri = null;
        if (prestoResponse.hasNonNull("nextUri")) {
            nextPageUri = ServletUriComponentsBuilder.fromCurrentContextPath()
                    .path(String.format(NEXT_PAGE_PATH_TEMPLATE, URI.create(prestoResponse.get("nextUri").asText()).getPath()))
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
                props.put("type", "array" );
                props.put("items", Map.of("type", JsonAdapter.toJsonType(type)));
            } else {
                props.put("type", JsonAdapter.toJsonType(type));
            }

            props.put("x-ga4gh-position", position++);
            schemaJson.put(column.get("name").asText(), props);
        }

        return schemaJson;
    }

    private List<String> getPrestoSchemas() {
        TableData data = search("show catalogs");
        List<String> catalogs = new ArrayList<>();
        for (Map<String, Object> catalog : data.getData()) {
            catalogs.add(catalog.get("Catalog").toString());
        }
        Map<String, List<String>> catalogSchemas = new HashMap<>();
        for (String catalog : catalogs) {
            List<String> schemas = new ArrayList<>();
            TableData schemaData = search("show schemas from " + catalog);
            for (Map<String, Object> schema : schemaData.getData()) {
                schemas.add(schema.get("Schema").toString());
            }
            catalogSchemas.put(catalog, schemas);
        }
        List<String> qualifiedSchemas = new ArrayList<>();
        catalogSchemas.forEach((catalog, schemas) -> {
            for (String schema : schemas) {
                qualifiedSchemas.add(catalog + "." + schema);
            }
        });

        return qualifiedSchemas;
    }
}
