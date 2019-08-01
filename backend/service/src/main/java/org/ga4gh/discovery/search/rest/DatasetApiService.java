package org.ga4gh.discovery.search.rest;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.ga4gh.dataset.DatasetManager;
import org.ga4gh.dataset.DatasetSource;
import org.ga4gh.dataset.SchemaId;
import org.ga4gh.dataset.SchemaIdConverter;
import org.ga4gh.dataset.SchemaManager;
import org.ga4gh.dataset.exception.DatasetNotFoundException;
import org.ga4gh.dataset.exception.SchemaNotFoundException;
import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.dataset.model.ListDatasetsResponse;
import org.ga4gh.dataset.model.ListSchemasResponse;
import org.ga4gh.dataset.model.Schema;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.fasterxml.jackson.databind.Module;

public class DatasetApiService {

    private int pageSize;
    private String datasetApiForceScheme;
    private SchemaManager schemaManager;
    private DatasetManager datasetManager;

    public DatasetApiService(
            String rootUrl, String localNS, String datasetApiForceScheme, int pageSize) {
        this.pageSize = pageSize;
        this.datasetApiForceScheme = datasetApiForceScheme;
        this.schemaManager = new SchemaManager(SchemaIdConverter.of(rootUrl, localNS));
        try {
            this.datasetManager = new DatasetManager(new URI(rootUrl), schemaManager);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public void initialize() {
        schemaManager.registerSchema(schemaStream("pgpc-schemas-0.1.0"));
        schemaManager.registerSchema(schemaStream("variant-object-0.1.0"));
        schemaManager.registerSchema(schemaStream("fact-object-0.1.0"));
        schemaManager.registerSchema(schemaStream("ga4gh-drs-0.1.0"));
        schemaManager.registerSchema(schemaStream("ga4gh-drs-object-0.1.0"));
    }

    private InputStream schemaStream(String schemaName) {
        return this.getClass().getResourceAsStream("/schemas/" + schemaName + ".json");
    }

    public ListSchemasResponse listSchemas() {
        return schemaManager.listSchemasResponse();
    }

    public ListDatasetsResponse listDatasets() {
        return datasetManager.listDatasetsResponse();
    }

    private String ensurePaginationToken(String paginationToken) {
        if (paginationToken == null) {
            paginationToken = PaginationToken.of(0, pageSize).encode();
        }
        return paginationToken;
    }

    public Dataset getDatasetResponse(String datasetId, String paginationToken) {
        Dataset dataset =
                datasetManager.getDatasetResponse(
                        datasetId, ensurePaginationToken(paginationToken), getRootUrl());
        if (dataset == null) {
            throw new DatasetNotFoundException("Dataset \"" + datasetId + "\" not found");
        }
        return dataset;
    }

    public void registerDataset(
            String datasetId, String description, String schemaId, DatasetSource datasetSource) {
        datasetManager.registerDataset(datasetId, description, schemaId, datasetSource);
    }

    public Module getObjectMapperModule() {
        return schemaManager.getObjectMapperModule();
    }

    public Schema getSchema(String schemaId) {
        Schema schema = schemaManager.getSchema(SchemaId.of(schemaId));
        if (schema == null) {
            throw new SchemaNotFoundException("Schema \"" + schemaId + "\" not found");
        }
        return schema;
    }

    public Schema getSchemaServerResponse(String schemaIdParam) {
        SchemaId schemaId = SchemaId.of(schemaIdParam);
        Schema schema = schemaManager.getSchemaServerResponse(schemaId, getRootUrl());
        if (schema == null) {
            throw new SchemaNotFoundException("Schema \"" + schemaIdParam + "\" not found");
        }
        return schema;
    }

    private URI getRootUrl() {
        ServletUriComponentsBuilder uriBuilder = ServletUriComponentsBuilder.fromCurrentRequest();
        if (datasetApiForceScheme != null) {
            uriBuilder.scheme(datasetApiForceScheme);
        }

        return uriBuilder.replacePath("/data/v1").replaceQuery(null).build().toUri();
    }
}
