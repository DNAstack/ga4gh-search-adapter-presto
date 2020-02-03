package com.dnastack.ga4gh.search.adapter.monitoring;

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

    private static MeterRegistry registry;
    private static final Map<String, Counter> counters = new HashMap<>();
    private static boolean initialized = false;
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
        if (this.config.getStackDriver() != null) {
            StackdriverConfig stackdriverConfig = configureStackDriver(this.config.getStackDriver().getProjectId());
            registry = StackdriverMeterRegistry.builder(stackdriverConfig).build();
            environment = this.config.getEnvironment();
        }

        if (environment == null || environment.trim().isEmpty()) {
            throw new RuntimeException("Failed to initialize monitor as environment was not populated.");
        }

        registry.config().commonTags("environment", environment);
        log.info("Finished setting up registry successfully.");
        initialized = true;
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
}
