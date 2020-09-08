package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoBadlyQualifiedNameException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoInsufficientResourcesException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoInternalErrorException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoInvalidQueryException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoNoSuchCatalogException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoNoSuchColumnException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoNoSuchSchemaException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoNoSuchTableException;
import com.dnastack.ga4gh.search.tables.ColumnSchema;
import com.dnastack.ga4gh.search.tables.DataModel;
import com.dnastack.ga4gh.search.tables.PageIndexEntry;
import com.dnastack.ga4gh.search.tables.Pagination;
import com.dnastack.ga4gh.search.tables.TableData;
import com.dnastack.ga4gh.search.tables.TableInfo;
import com.dnastack.ga4gh.search.tables.TablesList;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class SearchAdapter {
    private static final String NEXT_PAGE_SEARCH_TEMPLATE = "/search/%s"; //todo: alternatives?
    private static final String NEXT_PAGE_CATALOG_TEMPLATE = "/tables/catalog/%s";
    private static final URI JSON_SCHEMA_DRAFT7_URI = URI.create("http://json-schema.org/draft-07/schema#");

    //Matches the given name against the pattern <catalog>.<schema>.<table>, "<catalog>"."<schema>"."<table>", or
    //"<catalog>.<schema>.<table>".  Note this pattern is permissive and will often allow misquoted names through.
    private static final Pattern qualifiedNameMatcher =
            Pattern.compile("^\"?[^\"]+\"?\\.\"?[^\"]+\"?\\.\"?[^\"]+\"?$");

    private final PrestoClient client;
    private final HttpServletRequest request;
    private final Map<String, String> extraCredentials;
    private final Set<String> hiddenCatalogs;

    public SearchAdapter(HttpServletRequest request, PrestoClient prestoClient, Map<String, String> extraCredentials, Set<String> hiddenCatalogs) {
        this.client = prestoClient;
        this.extraCredentials = extraCredentials;
        this.request = request;
        this.hiddenCatalogs = hiddenCatalogs;
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
        throw new PrestoNoSuchCatalogException("No such catalog " + catalog);
    }


    private static String quote(String sqlIdentifier) {
        return "\"" + sqlIdentifier.replace("\"", "\"\"") + "\"";
    }

    public TableData getTableData(String tableName, String refHost) {
        TableData tableData = search("SELECT * FROM " + tableName);
        if(tableData.getDataModel() != null) { //only fill in the id if the data model is actually ready.
            tableData.getDataModel().setId(URI.create(String.format("%s/table/%s/info", refHost, tableName)));
            attachCommentsToDataModel(tableData, tableName);
        }
        return tableData;
    }


    private boolean isValidPrestoName(String tableName){
        return qualifiedNameMatcher.matcher(tableName).matches();
    }

    public TableInfo getTableInfo(String tableName, String refHost){
        if(!isValidPrestoName(tableName)){
            //triggers a 404.
           throw new PrestoBadlyQualifiedNameException("Invalid tablename "+tableName+" -- expected name in format <catalog>.<schema>.<tableName>");
        }
        TableData tableData = searchAll("SELECT * FROM " + tableName + " LIMIT 1");
        tableData.getDataModel().setId(URI.create(String.format("%s/table/%s/info", refHost, tableName)));
        attachCommentsToDataModel(tableData, tableName);
        return new TableInfo(tableName, null, tableData.getDataModel());

    }


    private TableData toTableData(String nextPageTemplate, JsonNode prestoResponse) {

        JsonNode columns;

        List<Map<String, Object>> data = new ArrayList<>();
        DataModel dataModel = null;
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

            dataModel = generateDataModel(columns);


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

        }else if(prestoResponse.hasNonNull("error")){
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                PrestoError prestoError = objectMapper.readValue(prestoResponse.get("error").toString(), PrestoError.class);
                if(prestoError.getErrorName().equals("CATALOG_NOT_FOUND")){
                    throw new PrestoNoSuchCatalogException(prestoError);
                }else if(prestoError.getErrorName().equals("SCHEMA_NOT_FOUND")){
                    throw new PrestoNoSuchSchemaException(prestoError);
                }else if(prestoError.getErrorName().equals("TABLE_NOT_FOUND")){
                    throw new PrestoNoSuchTableException(prestoError);
                }else if(prestoError.getErrorName().equals("COLUMN_NOT_FOUND")){
                    throw new PrestoNoSuchColumnException(prestoError);
                }else if(prestoError.getErrorType().equals("USER_ERROR")){
                    //Most other USER_ERRORs are bad queries and should likely return BAD_REQUEST error code.
                    throw new PrestoInvalidQueryException(prestoError);
                }else if(prestoError.getErrorType().equals("INSUFFICIENT_RESOURCES")){
                    throw new PrestoInsufficientResourcesException(prestoError);
                }else{
                    // as of this commit, the remaining presto error type is 'internal error', but this
                    // will also be a catch all.
                    throw new PrestoInternalErrorException(prestoError);
                }
            }catch(IOException ex){
                throw new UncheckedIOException(ex);
            }

        }

        // Generate pagination
        Pagination pagination = generatePagination(nextPageTemplate, prestoResponse);

        return new TableData(dataModel, Collections.unmodifiableList(data), pagination);
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


    private DataModel generateDataModel(JsonNode columns) {
        return DataModel.builder()
                 .id(null)
                 .description("Automatically generated schema")
                 .schema(JSON_SCHEMA_DRAFT7_URI)
                 .properties(getJsonSchemaProperties(columns))
                 .build();
    }

    private Map<String, ColumnSchema> getJsonSchemaProperties(JsonNode columns) {
        Map<String, ColumnSchema> schemaJson = new LinkedHashMap<>();

        for (JsonNode column : columns) {
            ColumnSchema columnSchema = new ColumnSchema();
            String type = column.get("type").asText();
            String format = JsonAdapter.toFormat(type);
            if (JsonAdapter.isArray(type)) {
                columnSchema.setType("array");
                if(format == null) {
                    columnSchema.setItems(
                            ColumnSchema.builder()
                                .type(JsonAdapter.toJsonType(type))
                                .build());

                }else{
                    columnSchema.setItems(
                            ColumnSchema.builder()
                                        .type(JsonAdapter.toJsonType(type))
                                        .format(format)
                                        .build());
                }
            } else {
                columnSchema.setType(JsonAdapter.toJsonType(type));
                if(format != null){
                    columnSchema.setFormat(format);
                }
            }

            schemaJson.put(column.get("name").asText(), columnSchema);
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
            if(hiddenCatalogs.contains(catalog.toLowerCase())){
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

    private void attachCommentsToDataModel(TableData tableData, String tableName) {
        if (tableData.getDataModel() == null) {
            return;
        }

        Map<String, ColumnSchema> dataModelProperties = tableData.getDataModel().getProperties();

        if (dataModelProperties == null) {
            return;
        }

        TableData describeData = searchAll("DESCRIBE " + tableName);

        for (Map<String, Object> describeRow : describeData.getData()) {
            final String columnName = (String) describeRow.get("Column");
            final String comment = (String) describeRow.get("Comment");

            if (dataModelProperties.containsKey(columnName) && comment != null && !comment.isBlank()) {
                dataModelProperties.get(columnName).setComment(comment);
            }
        }
    }
}
