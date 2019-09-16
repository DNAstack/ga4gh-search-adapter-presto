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

public class SchemaService {

    private int pageSize;
    private SchemaManager schemaManager;
    //TODO: better name
    private Map<String, String> registeredSchemas;

    SchemaService(
            String rootUrl, String localNS, int pageSize) {
        this.pageSize = pageSize;
        this.schemaManager = new SchemaManager(SchemaIdConverter.of(rootUrl, localNS));
        this.registeredSchemas = new HashMap<>();
    }

    /**
     * Registers a schema and the association between a qualified table name and its schema.
     * @param tableQualifiedName The fully-qualified table identifier.
     * @param schema_filename The file describing the schema associated with the table.
     */
    void registerSchema(String tableQualifiedName, String schema_filename) {
        Schema registeredSchema = registerSchema(schema_filename);
        registeredSchemas.put(tableQualifiedName, registeredSchema.getSchemaId().toString());
    }

    /**
     * Register a schema without a corresponding table
     * TODO: There is a word for this. Meta schema? Schema of schemas?
     * @param schema_filename The file describing the schema.
     * @return
     */
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

    Schema getSchema(String schemaId) {
        String internalSchemaId = registeredSchemas.getOrDefault(schemaId, schemaId);
        Schema schema = schemaManager.getSchema(SchemaId.of(internalSchemaId));
        if (schema == null) {
            throw new SchemaNotFoundException("Schema \"" + internalSchemaId + "\" not found");
        }
        return schema;
    }
}

