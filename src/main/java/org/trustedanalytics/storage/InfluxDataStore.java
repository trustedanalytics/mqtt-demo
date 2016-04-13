/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.storage;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class InfluxDataStore implements DataStore {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDataStore.class);

    private static final String SERIE_NAME = "classification";
    private static final String COLUMN_NAME = "class";
    private final String databaseName;
    private final String defaultGroupingInterval;
    private final String defaultTimeLimit;

    private InfluxDB store;

    public InfluxDataStore(String apiUrl, String username, String password, String databaseName,
        String defaultGroupingInterval, String defaultTimeLimit) {

        this.store = InfluxDBFactory.connect(apiUrl, username, password);
        this.databaseName = databaseName;
        this.defaultGroupingInterval = defaultGroupingInterval;
        this.defaultTimeLimit = defaultTimeLimit;
        initializeDatabase();
    }

    private void initializeDatabase() {
        if (!databaseExists()) {
            createDatabase();
        }
    }

    private boolean databaseExists() {
        LOG.debug("Check if database exists.");
        return store.describeDatabases().stream().filter(d -> d.getName().equals(databaseName))
            .count() > 0;
    }

    private void createDatabase() {
        LOG.debug("Creating database.");
        store.createDatabase(databaseName);
    }

    @Override
    public void save(double value) {
        save(SERIE_NAME, COLUMN_NAME, value);
    }

    private void save(String serieName, String key, double value) {
        LOG.debug("Save value '{}'='{}' in serie '{}'", key, String.valueOf(value), serieName);

        Serie serie = new Serie.Builder(serieName)
            .columns(key)
            .values(value)
            .build();

        write(serie);
    }

    private void write(Serie serie) {
        store.write(databaseName, TimeUnit.MILLISECONDS, serie);
    }

    @Override public Map<Date, Map<Double, Double>> read(String since, String groupBy) {
        return read(SERIE_NAME, COLUMN_NAME,
            Optional.ofNullable(groupBy).orElse(defaultGroupingInterval),
            Optional.ofNullable(since).orElse(defaultTimeLimit));
    }

    private Map<Date, Map<Double, Double>> read(String serieName, String key,
        String groupingInterval, String timeLimit) {

        String query =
            String.format("select count(%s) from %s group by time(%s), %s where time > now() - %s",
                key, serieName, groupingInterval, key, timeLimit);
        LOG.debug(query);

        List<Serie> queryResult = store.query(databaseName, query, TimeUnit.MILLISECONDS);
        LOG.debug("{} series read", queryResult.size());
        if (queryResult.isEmpty()) {
            return null;
        }

        LOG.debug("{} rows read in first serie", queryResult.get(0).getRows().size());
        return queryResult.get(0).getRows().stream()
            .map(row -> new SpaceShuttleRecord((Double) row.get("time"), (Double) row.get("class"),
                (Double) row.get("count")))
            .collect(Collectors.groupingBy(SpaceShuttleRecord::getTimestamp,
                SpaceShuttleRecordCollector.collect()));
    }
}
