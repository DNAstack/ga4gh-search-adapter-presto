package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.tables.Pagination;
import com.dnastack.ga4gh.search.tables.TableData;
import com.dnastack.ga4gh.search.tables.TableInfo;
import com.dnastack.ga4gh.search.tables.TablesList;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class SearchAdapter {

    private final static String NEXT_PAGE_PATH_TEMPLATE = "/search/%s"; //todo: alternatives?
    private final PrestoClient client;
    private final HttpServletRequest request;
    private final Map<String, String> extraCredentials;

    public SearchAdapter(HttpServletRequest request, PrestoClient prestoClient, Map<String, String> extraCredentials) {
        this.client = prestoClient;
        this.extraCredentials = extraCredentials;
        this.request = request;
    }

    private boolean hasMore(TableData tableData) {
        if (tableData.getPagination() != null && tableData.getPagination().getNextPageUrl() != null) {
            return true;
        }
        return false;
    }

    // Perform the given query and gather ALL results, by following Presto's nextUrl links
    public TableData searchAll(String statement){
        TableData tableData = search(statement);
        while(hasMore(tableData)){
            TableData nextPage = getNextPage(tableData.getPagination().getPrestoNextPageUrl().getPath());
            tableData.append(nextPage);
        }
        return tableData;
    }

    // Perform the given query, and return results.  Note the results may include 0 rows, even if there
    // is data available.  If the results include a non-null pagination object, then more data is
    // available at that URL.
    public TableData search(String statement){
        JsonNode response = client.query(statement, extraCredentials);
        return toTableData(response);

    }

    public TableData getNextPage(String page){
            JsonNode response = client.next(page, extraCredentials);
            return toTableData(response);

    }

    public TablesList getTables(String refHost){
        List<String> catalogs = getPrestoCatalogs();
        List<TablesList> tablesLists = catalogs.stream().map(catalog -> {
            log.trace("Getting schemas and tables for " + catalog);
            PrestoCatalog prestoCatalog = new PrestoCatalog(this, refHost, catalog);
            return prestoCatalog.getTablesList();
        }).collect(Collectors.toList());
        return new TablesList(tablesLists);
    }


    private static String quote(String sqlIdentifier) {
        return "\"" + sqlIdentifier.replace("\"", "\"\"") + "\"";
    }

    public TableData getTableData(String tableName, String refHost) {
        TableData tableData = search("SELECT * FROM " + tableName);
        if(tableData.getDataModel() != null) { //only fill in the id if the data model is actually ready.
            tableData.getDataModel().put("$id", String.format("%s/table/%s/info", refHost, tableName)); //todo: this could be better
        }
        return tableData;
    }

    public TableInfo getTableInfo(String tableName, String refHost){
        TableData tableData = searchAll("SELECT * FROM " + tableName + " LIMIT 1");
        tableData.getDataModel().put("$id", String.format("%s/table/%s/info", refHost, tableName));
        return new TableInfo(tableName, null, tableData.getDataModel());

    }


    private TableData toTableData(JsonNode prestoResponse) {

        JsonNode columns;
        Map<String, Object> generatedSchema = new LinkedHashMap<>();
        List<Map<String, Object>> data = new ArrayList<>();

        List<Transformer> transformers = new ArrayList<>();

        if (prestoResponse.hasNonNull("columns")) {
            // Generate data model
            columns = prestoResponse.get("columns");
            for(JsonNode column : columns){
                int i = 0;
                String type = column.get("type").asText();
                JsonNode typeSignature = column.get("typeSignature");
                String rawType = null;
                if(typeSignature != null){
                    rawType = typeSignature.get("rawType").asText();
                }
                transformers.add(JsonAdapter.getTransformer(type, rawType));
            }

            generatedSchema = generateDataModel(columns);


            // Generate data
            if (prestoResponse.hasNonNull("data")) {
                for (JsonNode dataNode : prestoResponse.get("data")) {
                    Map<String, Object> rowData = new LinkedHashMap<>();
                    for (int i = 0; i < dataNode.size(); i++) {
                        String content = dataNode.get(i).asText();
                        rowData.put(columns.get(i).get("name").asText(), transformers.get(i)!=null ? transformers.get(i).transform(content) : content);
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
        URI prestoNextPageUri = null;
        if (prestoResponse.hasNonNull("nextUri")) {
            prestoNextPageUri = ServletUriComponentsBuilder.fromHttpUrl(prestoResponse.get("nextUri").asText()).build().toUri();
            nextPageUri = ServletUriComponentsBuilder.fromContextPath(request)
                .path(String
                    .format(NEXT_PAGE_PATH_TEMPLATE, URI.create(prestoResponse.get("nextUri").asText()).getPath()))
                .build().toUri();
            log.info("Generating pagination as "+nextPageUri);
        }

        return new Pagination(nextPageUri, prestoNextPageUri);
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
            String format = JsonAdapter.toFormat(type);
            if (JsonAdapter.isArray(type)) {
                props.put("type", "array");
                if(format == null) {
                    props.put("items", Map.of("type", JsonAdapter.toJsonType(type)));
                }else{
                    props.put("items", Map.of("type", JsonAdapter.toJsonType(type), "format", format));
                }
            } else {
                props.put("type", JsonAdapter.toJsonType(type));
                if(format != null){
                    props.put("format", format);
                }
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
    private List<String> getPrestoCatalogs(){
        TableData catalogs = searchAll("show catalogs");
        List<String> catalogList = new ArrayList<>();
        for (Map<String, Object> row : catalogs.getData()) {
            String catalog = (String) row.get("Catalog");
            if (catalog.equalsIgnoreCase("system")) {
                log.debug("Ignoring catalog {}", catalog);
                continue;
            }
            log.debug("Found catalog {}", catalog);
            catalogList.add(catalog);
        }
        return catalogList;
    }
}
