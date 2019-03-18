package com.threathunter.persistent.core.util;

import com.threathunter.common.ShutdownHookManager;
import com.threathunter.metrics.aggregator.MetricsAggregator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
public class MetricsHelper {
    private static final MetricsHelper INSTANCE = new MetricsHelper();
    private static final String DEFAULT_DB = "nebula.online";

    private final MetricsAggregator aggregator;
    private final int expireSeconds;
    private volatile boolean running = false;

    private MetricsHelper() {
        this.aggregator = new MetricsAggregator();
        this.expireSeconds = 60 * 60 * 24 * 7;
        this.aggregator.initial(60);

        this.aggregator.startAggregator();
        this.running = true;

        ShutdownHookManager.get().addShutdownHook(this::stop, 101);
    }

    public static MetricsHelper getInstance() {
        return INSTANCE;
    }

    public void addMetrics(String db, String metricsName, Double value, String... tagKeyValues) {
        this.aggregator.add(db, metricsName, getBasicMap(tagKeyValues), value, expireSeconds);
    }

    public void addMetrics(String db, String metricsName, Map<String, Object> tags, Double value) {
        this.aggregator.add(db, metricsName, tags, value, expireSeconds);
    }

    public void addMetrics(String metricsName, Double value, String... tagKeyValues) {
        this.aggregator.add(DEFAULT_DB, metricsName, getBasicMap(tagKeyValues), value, expireSeconds);
    }

    public void addMetrics(String metricsName, Map<String, Object> tags, Double value) {
        this.aggregator.add(DEFAULT_DB, metricsName, tags, value, expireSeconds);
    }

    public void addMetrics(final String metricsName, final Double value) {
        this.aggregator.add(DEFAULT_DB, metricsName, Collections.emptyMap(), value, expireSeconds);
    }

    public void stop() {
        if (running) {
            this.aggregator.stopAggregator();
            this.running = false;
        }
    }

    private Map<String, Object> getBasicMap(final String... keyValues) {
        Map<String, Object> tags = new HashMap<>();
        for (int i = 0; i < keyValues.length - 1; i += 2) {
            tags.put(keyValues[i], keyValues[i + 1]);
        }
        return tags;
    }
}
