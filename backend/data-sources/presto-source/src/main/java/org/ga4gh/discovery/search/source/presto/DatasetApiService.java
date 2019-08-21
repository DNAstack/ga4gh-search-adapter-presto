package org.ga4gh.discovery.search.source.presto;

import org.ga4gh.dataset.SchemaId;
import org.ga4gh.dataset.SchemaIdConverter;
import org.ga4gh.dataset.SchemaManager;
import org.ga4gh.dataset.exception.SchemaNotFoundException;
import org.ga4gh.dataset.model.ListSchemasResponse;
import org.ga4gh.dataset.model.Schema;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class DatasetApiService {

    private int pageSize;
    private SchemaManager schemaManager;
    //TODO: better name
    private Map<String, String> ga4ghSchemas;

    public DatasetApiService(
            String rootUrl, String localNS, int pageSize) {
        this.pageSize = pageSize;
        this.schemaManager = new SchemaManager(SchemaIdConverter.of(rootUrl, localNS));
        this.ga4ghSchemas = new HashMap<>();
    }

    //TODO: better
    void registerSchema(String id, String schema_filename) {
        Schema registeredSchema = registerSchema(schema_filename);
        ga4ghSchemas.put(id, registeredSchema.getSchemaId().toString());
    }

    Schema registerSchema(String schema_filename) {
        return schemaManager.registerSchema(schemaStream(schema_filename));
    }

    private InputStream schemaStream(String schemaName) {
        return this.getClass().getResourceAsStream("/schemas/" + schemaName + ".json");
    }

    public ListSchemasResponse listSchemas() {
        return schemaManager.listSchemasResponse();
    }

    private String ensurePaginationToken(String paginationToken) {
        if (paginationToken == null) {
            paginationToken = PaginationToken.of(0, pageSize).encode();
        }
        return paginationToken;
    }

    public Schema getSchema(String schemaId) {
        String internalSchemaId = ga4ghSchemas.getOrDefault(schemaId, schemaId);
        Schema schema = schemaManager.getSchema(SchemaId.of(internalSchemaId));
        if (schema == null) {
            throw new SchemaNotFoundException("Schema \"" + internalSchemaId + "\" not found");
        }
        return schema;
    }
}

