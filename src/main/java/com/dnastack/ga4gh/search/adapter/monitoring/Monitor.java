package com.dnastack.ga4gh.search.adapter.monitoring;

import io.micrometer.azuremonitor.AzureMonitorConfig;
import io.micrometer.azuremonitor.AzureMonitorMeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.stackdriver.StackdriverConfig;
import io.micrometer.stackdriver.StackdriverMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class Monitor {

    private MeterRegistry registry;
    private final Map<String, Counter> counters = new HashMap<>();
    private boolean initialized = false;
    private MonitorConfig config;

    public Monitor(MonitorConfig config) {
        this.config = config;
    }

    public Counter getCounter(String counterName) {
        return counters.getOrDefault(counterName, null);
    }

    //TODO: Synchronized here puts some restrictions on how this can be used.
    // Is this too lazy a mechanism to stop races when registering counters?
    public synchronized Counter registerCounter(String counterName, String description, String... tags) {
        if (!initialized) {
            initialize();
        }

        if (counters.containsKey(counterName)) {
            throw new IllegalArgumentException("Counter " + counterName + " already exists.");
        }

        Counter counter = Counter
                .builder(counterName)
                .description(description)
                .tags(tags)
                .register(registry);
        counters.put(counterName, counter);
        log.info("Registered counter {}", counterName);
        return counter;
    }

    private synchronized void initialize() {
        if (initialized) {
            return;
        }

        String environment = null;
        if (config.getStackDriver() != null) {
            StackdriverConfig stackdriverConfig = configureStackDriver(config.getStackDriver().getProjectId());
            registry = StackdriverMeterRegistry.builder(stackdriverConfig).build();
            initialized = true;
        }

        if (config.getAzureMonitor() != null) {
            AzureMonitorConfig azureMonitorConfig = configureAzureMonitor();
            registry = AzureMonitorMeterRegistry.builder(azureMonitorConfig).build();
            initialized = true;
        }

        if (!initialized) {
            log.warn("Attempted to initialize monitoring with no registry configured. " +
                    "Re-initialization will not be attempted until the service restarts.");
            initialized = true;
            return;
        }

        if (config.getEnvironment() == null || config.getEnvironment().trim().isEmpty()) {
            throw new RuntimeException("Failed to initialize monitoring as environment was not populated.");
        }

        registry.config().commonTags("environment", config.getEnvironment());
        log.info("Finished setting up registry successfully.");
    }

    private StackdriverConfig configureStackDriver(String projectId) {
        return new StackdriverConfig() {
            @Override
            public String get(String s) {
                return null;
            }

            @Override
            public String projectId() {
                return projectId;
            }
        };
    }


    private AzureMonitorConfig configureAzureMonitor() {
        return s -> null;
    }
}
