package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.client.indexingservice.IndexingServiceClient;
import com.dnastack.ga4gh.search.client.indexingservice.model.LibraryItem;
import com.dnastack.ga4gh.search.client.tablesregistry.TablesRegistryClient;
import com.dnastack.ga4gh.search.client.tablesregistry.TablesRegistryClientConfig;
import com.dnastack.ga4gh.search.client.tablesregistry.model.ListTableRegistryEntry;
import com.dnastack.ga4gh.search.model.DataModel;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class PrestoTableDataModelSource {
    @Autowired(required = false)
    private DataModelSource dataModelSource;

    public DataModel getDataModel(String tableId) {
        if (this.dataModelSource == null) {
            return null;
        }
        return this.dataModelSource.getDataModel(tableId);
    }

    @Bean
    @ConditionalOnExpression("'${app.presto.tables.data-model-source}' == 'indexing-service'")
    private DataModelSource indexingServiceDataModelSource() {
        return new IndexingServiceDataModelSource();
    }

    @Bean
    @ConditionalOnExpression("'${app.presto.tables.data-model-source}' == 'tables-registry'")
    private DataModelSource tablesRegistryServiceDataModelSource() {
        return new TablesRegistryServiceDataModelSource();
    }

    private interface DataModelSource {
        DataModel getDataModel(String tableId);
    }

    @Slf4j
    private static class IndexingServiceDataModelSource implements DataModelSource {
        @Autowired
        private IndexingServiceClient indexingServiceClient;
        private final ObjectMapper objectMapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        @Override
        public DataModel getDataModel(String tableId) {
            try {
                LibraryItem item = indexingServiceClient.getBlob(tableId);

                if (item == null || item.getJsonSchema().isEmpty()) {
                    return null;
                }

                TypeReference<DataModel> type = new TypeReference<>() {};
                return objectMapper.readValue(item.getJsonSchema(), type);
            } catch (JsonParseException jsonParseException) {
                log.error("incorrectly formatted json schema", jsonParseException);
                return null;
            } catch (Exception e) {
                log.error("failed to fetch DataModel", e);
                return null;
            }
        }
    }

    private static class TablesRegistryServiceDataModelSource implements DataModelSource {
        @Autowired
        private TablesRegistryClient tablesRegistryClient;
        @Autowired
        private TablesRegistryClientConfig tablesRegistryClientConfig;

        @Override
        public DataModel getDataModel(String tableId) {
            ListTableRegistryEntry registryEntry = tablesRegistryClient.getTableRegistryEntry(
                    tablesRegistryClientConfig.getAuth().getClientId(),
                    tableId
            );

            if (registryEntry == null || registryEntry.getTableCollections().isEmpty()) {
                return null;
            }

            return registryEntry.getTableCollections().get(0).getTableSchema();
        }
    }
}
