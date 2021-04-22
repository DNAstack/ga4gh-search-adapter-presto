package com.dnastack.ga4gh.search.client.indexingservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class LibraryItem {
    String id;
    @NotBlank(message = "Non-blank value for type is required.")
    String type;
    @NotBlank(message = "Non-blank value for dataSource is required.")
    String dataSourceName;
    @NotBlank(message = "Non-blank value for dataSourceType is required.")
    String dataSourceType;
    @NotBlank(message = "Non-blank value for name is required.")
    String name;
    String description;
    String preferredName;
    List<String> aliases;
    Map<String, String> preferredColumnNames;
    String jsonSchema;
    List<DRSChecksum> checksums;
    List<DRSContentObject> bundleContents;
    Instant createdTime;
    Instant updatedTime;
    String mimeType;
    long size;
    String sizeUnit;
    String version;
    @NotBlank(message = "Non-blank value for dataSourceUrl is required.")
    String dataSourceUrl;
    Instant itemUpdatedTime;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class DRSChecksum {
        String checksum;
        String type;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class DRSContentObject {
        String name;
        String id;
        List<String> drsUri;
        List<DRSContentObject> contents;
    }
}
