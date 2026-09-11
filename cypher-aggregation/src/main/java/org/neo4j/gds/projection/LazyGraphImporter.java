/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.projection;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.ConfigKeyValidation;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.LazyIdMapBuilder;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.progress.registration.TaskStore;
import org.neo4j.gds.progress.tracking.BatchingTaskProgressTrackerFactory;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tracking.TaskProgressTracker;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LazyGraphImporter implements AutoCloseable {

    private final String username;
    private final DatabaseId databaseId;

    private final ExecutingQueryProvider queryProvider;
    private final QueryEstimator queryEstimator;
    private final Capabilities.WriteMode writeMode;

    private final Lock lock;

    private final GraphStoreCatalogService graphStoreCatalogService;
    private final RequestCorrelationId requestCorrelationId;
    private final TaskStore taskStore;
    private final Log log;
    private final ConfigValidator configValidator;

    // volatile: the fast path in initializeImporter is double-checked locking,
    // and getImporter() may be read from the thread calling result()
    private volatile @Nullable GraphImporter importer;
    private volatile @Nullable ProgressTracker progressTracker;

    private final AtomicBoolean closed;

    LazyGraphImporter(
        String username,
        DatabaseId databaseId,
        ExecutingQueryProvider queryProvider,
        QueryEstimator queryEstimator,
        Capabilities.WriteMode writeMode,
        GraphStoreCatalogService graphStoreCatalogService,
        RequestCorrelationId requestCorrelationId,
        TaskStore taskStore,
        Log log
    ) {
        this.username = username;
        this.databaseId = databaseId;
        this.queryProvider = queryProvider;
        this.queryEstimator = queryEstimator;
        this.writeMode = writeMode;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.requestCorrelationId = requestCorrelationId;
        this.taskStore = taskStore;
        this.log = log;
        this.lock = new ReentrantLock();
        this.configValidator = new ConfigValidator();
        this.closed = new AtomicBoolean(false);
    }

    GraphImporter initializeImporter(TextValue graphName, AnyValue config, AnyValue dataConfig, AnyValue migrationConfig) {
        var data = this.importer;
        if (data != null) {
            return data;
        }

        this.lock.lock();
        try {
            data = this.importer;
            if (data == null) {
                this.importer = data = createGraphImporter(graphName, config);
                this.configValidator.validateConfig(dataConfig, config, migrationConfig);
            }
            return data;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * The importer, or {@code null} if no row has been aggregated yet.
     * The caller of result() reads this from its own thread; the volatile
     * field guarantees it observes a fully-published importer.
     */
    @Nullable GraphImporter getImporter() {
        return importer;
    }

    /**
     * Failure cleanup: marks the progress task as failed and releases it.
     * Called once per aggregation updater on the error path; the guard makes
     * sure the tracker's failure handling runs only once.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            var tracker = this.progressTracker;
            if (tracker != null) {
                tracker.endSubTaskWithFailure();
            }
        }
    }

    private GraphImporter createGraphImporter(
        TextValue graphNameValue,
        AnyValue configMap
    ) {
        var graphName = graphNameValue.stringValue();
        var query = this.queryProvider.executingQuery().orElse("");

        validateGraphName(graphName, this.username, this.databaseId);
        var configMapValue = (configMap instanceof MapValue) ? (MapValue) configMap : MapValue.EMPTY;
        var config = GraphProjectFromCypherAggregationConfig.of(
            this.username,
            graphName,
            query,
            configMapValue
        );
        ConfigKeyValidation.requireOnlyKeysFrom(config.configKeys(), configMapValue.keySet());

        var idMapBuilder = idMapBuilder(config.readConcurrency());

        var taskVolume = queryEstimator.estimateRows(query);

        var taskRegistryFactory = TaskRegistryFactory.local(log, taskStore, new User(username, false));
        var taskRegistry = taskRegistryFactory.newInstance(config.jobId());

        var internalProgressTracker = TaskProgressTracker.create(
            log,
            new LoggerForProgressTrackingAdapter(log),
            GraphImporter.graphImporterTask(config.readConcurrency(), taskVolume),
            config.readConcurrency(),
            requestCorrelationId,
            taskRegistry
        );
        this.progressTracker = new BatchingTaskProgressTrackerFactory().create(internalProgressTracker, taskVolume, config.readConcurrency());

        return new GraphImporter(
            log,
            config,
            config.undirectedRelationshipTypes(),
            config.inverseIndexedRelationshipTypes(),
            idMapBuilder,
            writeMode,
            query,
            graphStoreCatalogService,
            progressTracker
        );
    }

    private static void validateGraphName(String graphName, String username, DatabaseId databaseId) {
        CypherMapAccess.failOnBlank("graphName", graphName);
        if (GraphStoreCatalog.exists(username, databaseId, graphName)) {
            throw new IllegalArgumentException("Graph " + graphName + " already exists");
        }
    }

    private static LazyIdMapBuilder idMapBuilder(Concurrency readConcurrency) {
        return new LazyIdMapBuilder(readConcurrency, true, true, PropertyState.PERSISTENT);
    }
}
