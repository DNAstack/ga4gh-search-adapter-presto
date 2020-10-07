package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.ApplicationConfig;
import com.dnastack.ga4gh.search.adapter.security.AuthConfig;
import com.dnastack.ga4gh.search.client.tablesregistry.OAuthClientConfig;
import com.dnastack.ga4gh.search.client.tablesregistry.TablesRegistryClient;
import com.dnastack.ga4gh.search.client.tablesregistry.model.ListTableRegistryEntry;
import com.dnastack.ga4gh.search.adapter.presto.exception.*;
import com.dnastack.ga4gh.search.repository.QueryJob;
import com.dnastack.ga4gh.search.repository.QueryJobRepository;
import com.dnastack.ga4gh.search.model.ColumnSchema;
import com.dnastack.ga4gh.search.model.DataModel;
import com.dnastack.ga4gh.search.model.PageIndexEntry;
import com.dnastack.ga4gh.search.model.Pagination;
import com.dnastack.ga4gh.search.model.TableData;
import com.dnastack.ga4gh.search.model.TableInfo;
import com.dnastack.ga4gh.search.model.TablesList;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@Configuration
public class PrestoSearchAdapter {
    private static final String NEXT_PAGE_SEARCH_TEMPLATE = "/search/%s"; //todo: alternatives?
    private static final String NEXT_PAGE_CATALOG_TEMPLATE = "/tables/catalog/%s";
    private static final URI JSON_SCHEMA_DRAFT7_URI = URI.create("http://json-schema.org/draft-07/schema#");

    //Matches the given name against the pattern <catalog>.<schema>.<table>, "<catalog>"."<schema>"."<table>", or
    //"<catalog>.<schema>.<table>".  Note this pattern is permissive and will often allow misquoted names through.
    private static final Pattern qualifiedNameMatcher =
            Pattern.compile("^\"?[^\"]+\"?\\.\"?[^\"]+\"?\\.\"?[^\"]+\"?$");

    @Autowired
    private PrestoClient client;

    @Autowired
    private ThrowableTransformer throwableTransformer;

    @Autowired
    private QueryJobRepository queryJobRepository;

    @Autowired
    private ApplicationConfig applicationConfig;

    @Autowired
    private TablesRegistryClient tablesRegistryClient;

    @Autowired
    private OAuthClientConfig oAuthClientConfig;

    private boolean hasMore(TableData tableData) {
        if (tableData.getPagination() != null && tableData.getPagination().getNextPageUrl() != null) {
            return true;
        }
        return false;
    }

    // Perform the given query and gather ALL results, by following Presto's nextUrl links
    // The query should NOT contain any functions that would not be recognized by Presto.
    TableData searchAll(String statement, HttpServletRequest request, Map<String, String> extraCredentials) {
        TableData tableData = search(statement, request, extraCredentials);
        while(hasMore(tableData)) {
            TableData nextPage = getNextSearchPage(tableData.getPagination().getPrestoNextPageUrl().getPath(), null, request, extraCredentials);
            tableData.append(nextPage);
        }
        return tableData;
    }

    // Pattern to match ga4gh_type two argument function
    private static final Pattern biFunctionPattern = Pattern.compile("((ga4gh_type)\\(\\s*([^,]+)\\s*,\\s*('[^']+')\\s*\\)((\\s+as)?\\s+((?!FROM\\s+)[A-Za-z0-9_]*))?)", Pattern.DOTALL|Pattern.CASE_INSENSITIVE);

    @Getter
    static class SQLFunction {
        final String functionName;
        final List<String> args = new ArrayList<>();
        final String columnAlias;

        public String getFunctionName() {
            return functionName;
        }
        public String getColumnAlias() {
            return columnAlias;
        }
        public List<String> getArgs() {
            return args;
        }
        public SQLFunction(MatchResult matchResult) {
            this.functionName = matchResult.group(2);
            for(int i = 3; i < matchResult.groupCount()-2; ++i) {
                this.args.add(matchResult.group(i));
            }
            this.columnAlias = matchResult.group(matchResult.groupCount());
            log.debug("Extracted function "+this.functionName+" with alias "+((columnAlias!=null) ? columnAlias : "null"));
        }

    }

    //rewrites the query by replacing all instances of functionName(a_0, a_1)
    //with a_argIndex
    private String rewriteQuery(String query, String functionName, int argIndex) {
        return biFunctionPattern.matcher(query)
                                .replaceAll(matchResult-> {
                           SQLFunction sf = new SQLFunction(matchResult);
                           if (sf.getFunctionName().equals(functionName)) {
                               String col = sf.getArgs().get(argIndex);
                               String alias = sf.getColumnAlias();
                               return (alias == null) ? col : col+" as "+alias;
                           }
                          return matchResult.group(1); //pass function through unchanged.
                       });
    }

    // Extracts all two-argument SQL functions from a query.
    private Stream<SQLFunction> parseSQLBiFunctions(String query) {
        Matcher matcher = biFunctionPattern.matcher(query);
        return matcher.results().map(SQLFunction::new);
    }

    // Given the parsed representation of the ga4gh_type function,
    // returns the type (second argument of the function), without quotes.
    // (this will be a JSON schema, or the shorthand $ref:<URL>)
    private String getGa4ghType(SQLFunction ga4ghFunction) {
        String ga4ghType = ga4ghFunction.getArgs().get(1).strip();
        if ((ga4ghType.startsWith("'") && ga4ghType.endsWith("'")) ||
           (ga4ghType.startsWith("\"") && ga4ghType.endsWith("\""))) {
            return ga4ghType.substring(1, ga4ghType.length()-1);
        } else {
            throw new QueryParsingException("Couldn't parse query: second argument to ga4gh_type must be quoted.");
        }
    }

    // Given tableData representing some search result, applies "type casting" of the result as described by the given
    // parsed ga4gh_type function.
    private void applyGa4ghTypeSqlFunction(SQLFunction ga4ghTypeFunction, TableData tableData) {
        ObjectMapper objectMapper = new ObjectMapper();
        DataModel dataModel = tableData.getDataModel();
        if (dataModel ==  null) {
            return;
        }

        if (dataModel.getRef() != null) {
            //sanity check
            throw new RuntimeException("Unable to apply SQL function to response with indirect $ref");
        }

        Map<String, ColumnSchema> columnSchemaMap = new HashMap<>(dataModel.getProperties());

        String columnName = (ga4ghTypeFunction.getColumnAlias()) != null ? ga4ghTypeFunction.getColumnAlias() : ga4ghTypeFunction.getArgs().get(0);
        String ga4ghType = getGa4ghType(ga4ghTypeFunction);

        ColumnSchema newColumnSchema;
        if (ga4ghType.startsWith("$ref:")) {
            String[] parts = ga4ghType.split(":",2);
            if (parts.length != 2) {
                //This could have been detected earlier, but whatever.
                throw new QueryParsingException("Unexpected second argument to ga4gh_type function, must be a valid JSON schema or the $ref:<URL> shorthand");
            }
            newColumnSchema = ColumnSchema.builder()
                    .ref(parts[1])
                    .build();
        } else {
            try {
                newColumnSchema = objectMapper.readValue(ga4ghType, ColumnSchema.class);
            } catch (IOException e) {
                throw new QueryParsingException("Unexpected second argument to ga4gh_type function, must be a valid JSON schema or the $ref:<URL> shorthand.", e);
            }
        }

        ColumnSchema columnSchema = columnSchemaMap.get(columnName);
        if (columnSchema == null) {
            throw new QueryParsingException("ga4gh_type was applied to column "+columnName+", but this column was not found in response.");
        } else {
            columnSchemaMap.put(columnName, newColumnSchema);
        }
        dataModel.setProperties(columnSchemaMap);
    }

    // Parses the saved query identified by queryJobId, finds all functions that transform the response in some way,
    // and applies them to the response represented by tableData.
    private void applyResponseTransforms(String queryJobId, final TableData tableData) {
        QueryJob queryJob = queryJobRepository.findById(queryJobId)
                                              .orElseThrow(()->new InvalidQueryJobException(queryJobId, "The query corresponding to this search could not be located."));
        String query = queryJob.getQuery();
        Stream<SQLFunction> responseTransformingFunctions = parseSQLBiFunctions(query);

        responseTransformingFunctions
                .filter(sqlFunction->sqlFunction.functionName.equals("ga4gh_type"))
                .forEach(sqlFunction-> applyGa4ghTypeSqlFunction(sqlFunction, tableData));
    }

    public TableData search(String query, HttpServletRequest request, Map<String, String> extraCredentials) {
        Stream<SQLFunction> responseTransformingFunctions = parseSQLBiFunctions(query);
        String rewrittenQuery = rewriteQuery(query, "ga4gh_type", 0);
        JsonNode response = client.query(rewrittenQuery, extraCredentials);
        QueryJob queryJob = QueryJob.builder()
                                    .query(query)
                                    .id(UUID.randomUUID().toString())
                                    .build();
        queryJob = queryJobRepository.save(queryJob);
        TableData tableData = toTableData(NEXT_PAGE_SEARCH_TEMPLATE, response, queryJob.getId(), request);
        responseTransformingFunctions
                .filter(sqlFunction->sqlFunction.functionName.equals("ga4gh_type"))
                .forEach(sqlFunction-> applyGa4ghTypeSqlFunction(sqlFunction, tableData));

        return tableData;
    }


    public TableData getNextSearchPage(String page, String queryJobId, HttpServletRequest request, Map<String, String> extraCredentials) {
            log.debug("getNextSearchPage");
            JsonNode response = client.next(page, extraCredentials);
            return toTableData(NEXT_PAGE_SEARCH_TEMPLATE, response, queryJobId, request);

    }

    private URI getLinkToCatalog(String catalog, HttpServletRequest request) {
        return ServletUriComponentsBuilder.fromContextPath(request)
                                   .path(String.format(NEXT_PAGE_CATALOG_TEMPLATE, catalog))
                                   .build().toUri();
    }

    private PageIndexEntry getPageIndexEntryForCatalog(String catalog, int page, HttpServletRequest request) {
        URI uri = getLinkToCatalog(catalog, request);
        return PageIndexEntry.builder()
                             .catalog(catalog)
                             .url(uri)
                             .page(page)
                             .build();
    }

    private String getRefHost() {
        return ServletUriComponentsBuilder.fromCurrentContextPath().toUriString();
    }

    private List<PageIndexEntry> getPageIndex(Set<String> catalogs, HttpServletRequest request) {
        final int[] page = {0};
        return catalogs.stream().map(catalog->getPageIndexEntryForCatalog(catalog, page[0]++, request)).collect(Collectors.toList());
    }

    private TablesList getTables(String currentCatalog, String nextCatalog, HttpServletRequest request, Map<String, String> extraCredentials) {
        PrestoCatalog prestoCatalog = new PrestoCatalog(this, throwableTransformer, getRefHost(), currentCatalog);
        Pagination nextPage = null;
        if (nextCatalog != null) {
            nextPage = new Pagination(null, getLinkToCatalog(nextCatalog, request), null);
        }

        TablesList tablesList = prestoCatalog.getTablesList(nextPage, request, extraCredentials);

        return tablesList;
    }

    public TablesList getTables(HttpServletRequest request, Map<String, String> extraCredentials) {
        Set<String> catalogs = getPrestoCatalogs(request, extraCredentials);
        if (catalogs == null || catalogs.isEmpty()) {
            return new TablesList(List.of(), null, null);
        }
        Iterator<String> catalogIt = catalogs.iterator();

        TablesList tablesList = getTables(catalogIt.next(), catalogIt.hasNext() ? catalogIt.next() : null, request, extraCredentials);
        tablesList.setIndex(getPageIndex(catalogs, request));
        return tablesList;
    }

    public TablesList getTablesInCatalog(String catalog, HttpServletRequest request, Map<String, String> extraCredentials) {
        Set<String> catalogs = getPrestoCatalogs(request, extraCredentials);
        if (catalogs != null) {
            Iterator<String> catalogIt = catalogs.iterator();
            while (catalogIt.hasNext()) {
                if (catalogIt.next().equals(catalog)) {
                    return getTables(catalog, catalogIt.hasNext() ? catalogIt.next() : null, request, extraCredentials);
                }
            }
        }
        throw new PrestoNoSuchCatalogException("No such catalog " + catalog);
    }


    private static String quote(String sqlIdentifier) {
        return "\"" + sqlIdentifier.replace("\"", "\"\"") + "\"";
    }

    public TableData getTableData(String tableName, HttpServletRequest request, Map<String, String> extraCredentials) {

        TableData tableData = search("SELECT * FROM " + tableName, request, extraCredentials);

        // Get table JSON schema from tables registry if one exists for this table (for tables from presto-public)
        DataModel dataModel = getDataModelFromTablesRegistry(tableName);

        // Otherwise extract dataModel from tableData
        if (dataModel == null & tableData.getDataModel() != null) {
            dataModel = tableData.getDataModel();
        }

        // Fill in the id & comments if the data model is ready
        if (dataModel != null) {
            dataModel.setId(getDataModelId(tableName));
            attachCommentsToDataModel(dataModel, tableName, request, extraCredentials);
        }
        return tableData;
    }


    private boolean isValidPrestoName(String tableName) {
        return qualifiedNameMatcher.matcher(tableName).matches();
    }

    public TableInfo getTableInfo(String tableName, HttpServletRequest request, Map<String, String> extraCredentials) {
        if (!isValidPrestoName(tableName)) {
            //triggers a 404.
           throw new PrestoBadlyQualifiedNameException("Invalid tablename "+tableName+" -- expected name in format <catalog>.<schema>.<tableName>");
        }

        TableData tableData = searchAll("SELECT * FROM " + tableName + " LIMIT 1", request, extraCredentials);

        // Get table JSON schema from tables registry if one exists for this table (for tables from presto-public)
        DataModel dataModel = getDataModelFromTablesRegistry(tableName);

        // Otherwise extract dataModel from tableData
        if (dataModel == null & tableData.getDataModel() != null) {
            dataModel = tableData.getDataModel();
        }

        // Fill in the id & comments if the data model is ready
        if (dataModel != null) {
            dataModel.setId(getDataModelId(tableName));
            attachCommentsToDataModel(dataModel, tableName, request, extraCredentials);
        }
        return new TableInfo(tableName, dataModel.getDescription(), dataModel, null);
    }

    private TableData toTableData(String nextPageTemplate, JsonNode prestoResponse, String queryJobId, HttpServletRequest request) {


        List<Map<String, Object>> data = new ArrayList<>();
        DataModel dataModel = null;
        List<PrestoDataTransformer> prestoDataTransformers = new ArrayList<>();

        if (prestoResponse.hasNonNull("columns")) {
            // Generate data model
            final JsonNode columns = prestoResponse.get("columns");
            for(JsonNode column : columns) {
                int i = 0;
                String type = column.get("type").asText();
                JsonNode typeSignature = column.get("typeSignature");
                String rawType = null;
                if (typeSignature != null) {
                    rawType = typeSignature.get("rawType").asText();
                }
                prestoDataTransformers.add(JsonAdapter.getPrestoDataTransformer(type, rawType));
            }

            dataModel = generateDataModel(columns);

            // Generate data
            if (prestoResponse.hasNonNull("data")) {
                for (JsonNode dataNode : prestoResponse.get("data")) {
                    Map<String, Object> rowData = new LinkedHashMap<>();
                    for (int i = 0; i < dataNode.size(); i++) {
                        String content = dataNode.get(i).asText();
                        rowData.put(columns.get(i).get("name").asText(), prestoDataTransformers.get(i) != null ? prestoDataTransformers
                                .get(i).transform(content) : content);
                    }
                    data.add(rowData);
                }
            }

        } else if (prestoResponse.hasNonNull("error")) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                PrestoError prestoError = objectMapper.readValue(prestoResponse.get("error").toString(), PrestoError.class);
                if (prestoError.getErrorName().equals("CATALOG_NOT_FOUND")) {
                    throw new PrestoNoSuchCatalogException(prestoError);
                } else if (prestoError.getErrorName().equals("SCHEMA_NOT_FOUND")) {
                    throw new PrestoNoSuchSchemaException(prestoError);
                } else if (prestoError.getErrorName().equals("TABLE_NOT_FOUND")) {
                    throw new PrestoNoSuchTableException(prestoError);
                } else if (prestoError.getErrorName().equals("COLUMN_NOT_FOUND")) {
                    throw new PrestoNoSuchColumnException(prestoError);
                } else if (prestoError.getErrorType().equals("USER_ERROR")) {
                    //Most other USER_ERRORs are bad queries and should likely return BAD_REQUEST error code.
                    throw new PrestoInvalidQueryException(prestoError);
                } else if (prestoError.getErrorType().equals("INSUFFICIENT_RESOURCES")) {
                    throw new PrestoInsufficientResourcesException(prestoError);
                } else {
                    // as of this commit, the remaining presto error type is 'internal error', but this
                    // will also be a catch all.
                    throw new PrestoInternalErrorException(prestoError);
                }
            } catch (IOException ex) {
                throw new UncheckedTableDataConstructionException(ex);
            }

        }

        // Generate pagination
        Pagination pagination = generatePagination(nextPageTemplate, prestoResponse, queryJobId, request);

        TableData tableData = new TableData(dataModel, Collections.unmodifiableList(data), null, pagination);
        if (queryJobId != null) {
            applyResponseTransforms(queryJobId, tableData);
        }
        return tableData;
    }

    private Pagination generatePagination(String template, JsonNode prestoResponse, String queryJobId, HttpServletRequest request) {
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

        return new Pagination(queryJobId, nextPageUri, prestoNextPageUri);
    }


    private DataModel generateDataModel(JsonNode columns) {
        return DataModel.builder()
                 .id(null)
                 .description("Automatically generated schema")
                 .schema(JSON_SCHEMA_DRAFT7_URI)
                 .properties(getJsonSchemaProperties(columns))
                 .build();
    }


    private ColumnSchema getColumnSchema(JsonNode value){
        final String rawType = value.get("rawType").asText();
        final String format = JsonAdapter.toFormat(value.get("rawType").asText());
        if(rawType.equalsIgnoreCase("array")) {
            ColumnSchema columnSchema = getColumnSchema(value.get("arguments").get(0).get("value"));
            return ColumnSchema.builder()
                    .type("array")
                    .comment("array["+columnSchema.getType()+"]")
                    .items(columnSchema)
                    .build();
        }else if(rawType.equalsIgnoreCase("row")) {
            JsonNode args = value.get("arguments");

            Map<String, ColumnSchema> m = StreamSupport.stream(args.spliterator(), false)
                         .collect(
                                 Collectors.toMap(rowArg->rowArg.get("value").get("fieldName").get("name").asText(),
                                                  rowArg->getColumnSchema(rowArg.get("value").get("typeSignature")),
                                                  (k,v)->{ throw new UnexpectedQueryResponseException("rows must have unique key names. Duplicate key "+k+", value="+v); },
                                                  LinkedHashMap::new)); //maintain key order to generate better comment.


            return ColumnSchema.builder()
                    .type("object")
                    .comment(String.format("row(%s)", Strings.join(
                            m.values().stream()
                             .map(cs->cs.getType())
                             .collect(Collectors.toList()), ',')))
                    .properties(m)
                    .build();
        }else if(rawType.equalsIgnoreCase("map")){

            ColumnSchema keySchema = getColumnSchema(value.get("arguments").get(0).get("value"));
            ColumnSchema valueSchema = getColumnSchema(value.get("arguments").get(1).get("value"));

            return ColumnSchema.builder()
                    .type("object")
                    .comment(String.format("map(%s, %s)", keySchema.getType(), valueSchema.getType()))
                    .properties(Map.of("key", keySchema, "value", valueSchema))
                    .build();

        }else if(rawType.equalsIgnoreCase("json")) {
            return ColumnSchema.builder()
                               .type("object")
                               .comment("json")
                               .build();
        }else {
            //must be a primitive.
            return ColumnSchema.builder()
                    .type(JsonAdapter.toJsonType(rawType))
                    .format(format)
                    .build();
        }

    }

    private Map<String, ColumnSchema> getJsonSchemaProperties(JsonNode columns) {

        return StreamSupport.stream(columns.spliterator(), false)
                     .map(column->{
                         return Map.entry(column.get("name").asText(), getColumnSchema(column.get("typeSignature")));
                     }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    }


    /**
     * Get a list of the catalogs served by the connected instance of PrestoSQL.
     *
     * @return A List of Strings, where each String is the name of the catalog.
     * @throws IOException If the query to enumerate the list of catalogs fails.
     */
    private Set<String> getPrestoCatalogs(HttpServletRequest request, Map<String, String> extraCredentials) {
        TableData catalogs = searchAll("select catalog_name FROM system.metadata.catalogs ORDER BY catalog_name", request, extraCredentials);
        Set<String> catalogSet = new LinkedHashSet<>();
        for (Map<String, Object> row : catalogs.getData()) {
            String catalog = (String) row.get("catalog_name");
            if (applicationConfig.getHiddenCatalogs().contains(catalog.toLowerCase())) {
                log.debug("Ignoring catalog {}", catalog);
                continue;
            }

            log.trace("Found catalog {}", catalog);
            if (catalogSet.contains(catalog)) {
                throw new AssertionError("Unexpected duplicate catalog "+catalog);
            }
            catalogSet.add(catalog);
        }
        return catalogSet;
    }

    private void attachCommentsToDataModel(DataModel dataModel, String tableName, HttpServletRequest request, Map<String, String> extraCredentials) {
        if (dataModel == null) {
            return;
        }

        Map<String, ColumnSchema> dataModelProperties = dataModel.getProperties();

        if (dataModelProperties == null) {
            return;
        }

        TableData describeData = searchAll("DESCRIBE " + tableName, request, extraCredentials);

        for (Map<String, Object> describeRow : describeData.getData()) {
            final String columnName = (String) describeRow.get("Column");
            final String comment = (String) describeRow.get("Comment");

            if (dataModelProperties.containsKey(columnName) && comment != null && !comment.isBlank()) {
                dataModelProperties.get(columnName).setComment(comment);
            }
        }
    }

    private URI getDataModelId(String tableName) {
        String refHost = ServletUriComponentsBuilder.fromCurrentContextPath().toUriString();
        return URI.create(String.format("%s/table/%s/info", refHost, tableName));
    }

    private DataModel getDataModelFromTablesRegistry(String tableName) {
        ListTableRegistryEntry registryEntry = tablesRegistryClient.getTableRegistryEntry(
                oAuthClientConfig.getClientId(), tableName);

        if(registryEntry == null || registryEntry.getTableCollections().isEmpty()) {
            return null;
        }

        return registryEntry.getTableCollections().get(0).getTableSchema();
    }
}
