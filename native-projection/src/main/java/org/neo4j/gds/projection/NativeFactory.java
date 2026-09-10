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

import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.Nodes;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.progress.tasks.Tasks;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tracking.TaskProgressTracker;
import org.neo4j.gds.progress.tracking.TaskTreeProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.internal.id.IdGeneratorFactory;

import java.util.concurrent.ExecutorService;

public final class NativeFactory extends CSRGraphStoreFactory<GraphProjectFromStoreConfig> {
    private final GraphProjectFromStoreConfig storeConfig;
    private final ProgressTracker progressTracker;
    private final ExecutorService executorService;
    private final TerminationFlag terminationFlag;

    static NativeFactory nativeFactory(
        GraphProjectFromStoreConfig graphProjectFromStoreConfig,
        GraphLoaderContext loadingContext,
        RequestCorrelationId requestCorrelationId,
        IdGeneratorFactory idGeneratorFactory,
        ExecutorService executorService
    ) {
        var graphDimensions = GraphDimensionsReader.graphDimensionsReader(
            loadingContext.transactionContext(),
            graphProjectFromStoreConfig,
            idGeneratorFactory
        ).call();

        return nativeFactory(graphProjectFromStoreConfig, loadingContext, requestCorrelationId, graphDimensions, executorService);
    }

    static NativeFactory nativeFactory(
        GraphProjectFromStoreConfig graphProjectFromStoreConfig,
        GraphLoaderContext loadingContext,
        RequestCorrelationId requestCorrelationId,
        GraphDimensions graphDimensions,
        ExecutorService executorService
    ) {
        var progressTracker = initProgressTracker(
            graphProjectFromStoreConfig,
            loadingContext.taskRegistryFactory(),
            loadingContext.log(),
            graphDimensions
        );
        return new NativeFactory(
            graphProjectFromStoreConfig,
            loadingContext,
            requestCorrelationId,
            graphDimensions,
            progressTracker,
            executorService
        );
    }

    // Package-private constructor used in tests
    NativeFactory(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        RequestCorrelationId requestCorrelationId,
        GraphDimensions graphDimensions,
        ProgressTracker progressTracker,
        ExecutorService executorService
    ) {
        super(
            graphProjectConfig,
            new Capabilities(Capabilities.WriteMode.LOCAL),
            loadingContext.transactionContext(),
            loadingContext.databaseId(),
            graphDimensions,
            loadingContext.log(),
            requestCorrelationId
        );
        this.storeConfig = graphProjectConfig;
        this.progressTracker = progressTracker;
        this.executorService = executorService;
        this.terminationFlag = loadingContext.terminationFlag();
    }

    @Override
    public MemoryEstimation estimateMemoryUsageDuringLoading() {
        return getMemoryEstimation(storeConfig.nodeProjections(), storeConfig.relationshipProjections(), true);
    }

    @Override
    public MemoryEstimation estimateMemoryUsageAfterLoading() {
        return getMemoryEstimation(storeConfig.nodeProjections(), storeConfig.relationshipProjections(), false);
    }

    private static ProgressTracker initProgressTracker(
        GraphProjectFromStoreConfig graphProjectConfig,
        TaskRegistryFactory taskRegistryFactory,
        Log log,
        GraphDimensions dimensions
    ) {
        long relationshipCount = graphProjectConfig
            .relationshipProjections()
            .projections()
            .entrySet()
            .stream()
            .map(entry -> {
                long relCount = entry.getKey().name.equals("*")
                    ? dimensions.relationshipCounts().values().stream().reduce(Long::sum).orElse(0L)
                    : dimensions.relationshipCounts().getOrDefault(entry.getKey(), 0L);

                return entry.getValue().orientation() == Orientation.UNDIRECTED
                    ? relCount * 2
                    : relCount;
            }).mapToLong(Long::longValue).sum();

        var concurrency = graphProjectConfig.readConcurrency();

        var task = Tasks.task(
            "Loading",
            concurrency, Tasks.task("Nodes", concurrency, Tasks.leaf("Store Scan", concurrency, dimensions.nodeCount())),
            Tasks.task("Relationships", concurrency, Tasks.leaf("Store Scan", concurrency, relationshipCount))
        );

        if (graphProjectConfig.logProgress()) {
            var taskRegistry = taskRegistryFactory.newInstance(graphProjectConfig.jobId());

            return TaskProgressTracker.create(
                log,
                new LoggerForProgressTrackingAdapter(log),
                task,
                concurrency,
                PlainSimpleRequestCorrelationId.create(),
                taskRegistry
            );
        }

        return TaskTreeProgressTracker.create(
            log,
            new LoggerForProgressTrackingAdapter(log),
            task,
            concurrency,
            graphProjectConfig.jobId(),
            PlainSimpleRequestCorrelationId.create(),
            taskRegistryFactory
        );
    }

    @Override
    public CSRGraphStore build() {
        try {
            // Start the sub-task so it will be registered in the task store
            progressTracker.beginSubTask();
            // `validate` can raise an exception which has to be handled in order to end the sub-task with failure
            validate();
            var concurrency = graphProjectConfig.readConcurrency();
            Nodes nodes = loadNodes(concurrency);
            RelationshipImportResult relationships = loadRelationships(nodes.idMap(), concurrency);
            CSRGraphStore graphStore = createGraphStore(nodes, relationships);

            logLoadingSummary(graphStore);
            progressTracker.endSubTask();

            return graphStore;
        } catch (Exception e) {
            progressTracker.endSubTaskWithFailure();
            throw e;
        }
    }

    // Allows for mocking the validation behaviour.
    void validate() throws IllegalArgumentException {
        GraphDimensionsValidation.validate(dimensions, storeConfig);
    }

    private Nodes loadNodes(Concurrency concurrency) {
        var scanningNodesImporter = ScanningNodesImporter.scanningNodesImporter(
            graphProjectConfig,
            log,
            transactionContext,
            terminationFlag,
            dimensions,
            progressTracker,
            executorService,
            concurrency
        );

        try {
            progressTracker.beginSubTask();
            var result = scanningNodesImporter.call();
            progressTracker.endSubTask();

            return result;
        } catch (Exception e) {
            progressTracker.endSubTaskWithFailure();
            throw e;
        }
    }

    private RelationshipImportResult loadRelationships(IdMap idMap, Concurrency concurrency) {
        var scanningRelationshipsImporter = ScanningRelationshipsImporter.scanningRelationshipsImporter(
            graphProjectConfig,
            log,
            transactionContext,
            terminationFlag,
            dimensions,
            progressTracker,
            executorService,
            concurrency,
            idMap
        );

        try {
            progressTracker.beginSubTask();
            var result = scanningRelationshipsImporter.call();
            progressTracker.endSubTask();

            return result;
        } catch (Exception e) {
            progressTracker.endSubTaskWithFailure();
            throw e;
        }
    }

    @Override
    protected ProgressTracker progressTracker() {
        return progressTracker;
    }
}
