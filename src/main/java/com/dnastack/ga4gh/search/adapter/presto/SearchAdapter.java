package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.tables.PageIndexEntry;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class SearchAdapter {
    private static final String NEXT_PAGE_SEARCH_TEMPLATE = "/search/%s"; //todo: alternatives?
    private static final String NEXT_PAGE_CATALOG_TEMPLATE = "/tables/catalog/%s";

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
            TableData nextPage = getNextSearchPage(tableData.getPagination().getPrestoNextPageUrl().getPath());
            tableData.append(nextPage);
        }
        return tableData;
    }

    TableData search(String statement){
        JsonNode response = client.query(statement, extraCredentials);
        return toTableData(NEXT_PAGE_SEARCH_TEMPLATE, response);
    }


    public TableData getNextSearchPage(String page){
            JsonNode response = client.next(page, extraCredentials);
            return toTableData(NEXT_PAGE_SEARCH_TEMPLATE, response);

    }

    private URI getLinkToCatalog(String catalog){
        return ServletUriComponentsBuilder.fromContextPath(request)
                                   .path(String.format(NEXT_PAGE_CATALOG_TEMPLATE, catalog))
                                   .build().toUri();
    }

    private PageIndexEntry getPageIndexEntryForCatalog(String catalog, int page){
        URI uri = getLinkToCatalog(catalog);
        return PageIndexEntry.builder()
                             .catalog(catalog)
                             .url(uri)
                             .page(page)
                             .build();
    }

    private String getRefHost(){
        return ServletUriComponentsBuilder.fromCurrentContextPath().toUriString();
    }

    private List<PageIndexEntry> getPageIndex(Set<String> catalogs){
        final int[] page = {0};
        return catalogs.stream().map(catalog->getPageIndexEntryForCatalog(catalog, page[0]++)).collect(Collectors.toList());
    }

    private TablesList getTables(String currentCatalog, String nextCatalog){
        PrestoCatalog prestoCatalog = new PrestoCatalog(this, getRefHost(), currentCatalog);
        Pagination nextPage = null;
        if(nextCatalog != null){
            nextPage = new Pagination(getLinkToCatalog(nextCatalog), null);
        }

        TablesList tablesList = prestoCatalog.getTablesList(nextPage);

        return tablesList;
    }

    public TablesList getTables(){
        Set<String> catalogs = getPrestoCatalogs();
        if(catalogs == null || catalogs.isEmpty()){
            return new TablesList(List.of(), null, null);
        }
        Iterator<String> catalogIt = catalogs.iterator();

        TablesList tablesList = getTables(catalogIt.next(), catalogIt.hasNext() ? catalogIt.next() : null);
        tablesList.setIndex(getPageIndex(catalogs));
        return tablesList;
    }

    public TablesList getTablesInCatalog(String catalog){
        Set<String> catalogs = getPrestoCatalogs();
        if(catalogs != null) {
            Iterator<String> catalogIt = catalogs.iterator();
            while (catalogIt.hasNext()) {
                if (catalogIt.next().equals(catalog)) {
                    return getTables(catalog, catalogIt.hasNext() ? catalogIt.next() : null);
                }
            }
        }
        throw new PrestoNoSuchCatalogException("No such catalog "+catalog);
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


    private TableData toTableData(String nextPageTemplate, JsonNode prestoResponse) {

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
        Pagination pagination = generatePagination(nextPageTemplate, prestoResponse);

        return new TableData(generatedSchema, Collections.unmodifiableList(data), pagination);
    }

    private Pagination generatePagination(String template, JsonNode prestoResponse) {
        URI nextPageUri = null;
        URI prestoNextPageUri = null;
        if (prestoResponse.hasNonNull("nextUri")) {
            prestoNextPageUri = ServletUriComponentsBuilder.fromHttpUrl(prestoResponse.get("nextUri").asText()).build().toUri();
            nextPageUri = ServletUriComponentsBuilder.fromContextPath(request)
                .path(String
                    .format(template, URI.create(prestoResponse.get("nextUri").asText()).getPath()))
                .build().toUri();
            log.debug("Generating pagination as "+nextPageUri);
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
    private Set<String> getPrestoCatalogs(){
        TableData catalogs = searchAll("select catalog_name FROM system.metadata.catalogs ORDER BY catalog_name");
        Set<String> catalogSet = new LinkedHashSet<>();
        for (Map<String, Object> row : catalogs.getData()) {
            String catalog = (String) row.get("catalog_name");
            if (catalog.equalsIgnoreCase("system")) {
                log.debug("Ignoring catalog {}", catalog);
                continue;
            }
            log.trace("Found catalog {}", catalog);
            if(catalogSet.contains(catalog)){
                throw new AssertionError("Unexpected duplicate catalog "+catalog);
            }
            catalogSet.add(catalog);
        }
        return catalogSet;
    }
}
