package com.dnastack.ga4gh.search.adapter.monitoring;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "management.metrics.export")
public class MonitorConfig {

    String environment = null;
    StackDriver stackDriver = null;
    AzureMonitor azureMonitor = null;

    @Getter
    @Setter
    public static class StackDriver {
        String projectId;
    }

    @Getter
    @Setter
    public static class AzureMonitor {
        boolean enabled;
    }
}