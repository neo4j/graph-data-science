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

import org.immutables.builder.Builder;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.loading.Nodes;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskTreeProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.mem.MemoryEstimation;

import java.util.Optional;

import static org.neo4j.gds.projection.GraphDimensionsValidation.validate;

final class NativeFactory extends CSRGraphStoreFactory<GraphProjectFromStoreConfig> {

    private final GraphProjectFromStoreConfig storeConfig;
    private final ProgressTracker progressTracker;

    @Builder.Factory
    static NativeFactory nativeFactory(
        GraphProjectFromStoreConfig graphProjectFromStoreConfig,
        GraphLoaderContext loadingContext,
        Optional<GraphDimensions> graphDimensions
    ) {
        var dimensions = graphDimensions.orElseGet(() -> new GraphDimensionsReaderBuilder()
            .graphLoaderContext(loadingContext)
            .graphProjectConfig(graphProjectFromStoreConfig)
            .build()
            .call()
        );

        return new NativeFactory(
            graphProjectFromStoreConfig,
            loadingContext,
            dimensions
        );
    }

    private NativeFactory(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions graphDimensions
    ) {
        super(graphProjectConfig, ImmutableStaticCapabilities.of(WriteMode.LOCAL), loadingContext, graphDimensions);
        this.storeConfig = graphProjectConfig;
        this.progressTracker = initProgressTracker();
    }

    @Override
    public MemoryEstimation estimateMemoryUsageDuringLoading() {
        return getMemoryEstimation(storeConfig.nodeProjections(), storeConfig.relationshipProjections(), true);
    }

    @Override
    public MemoryEstimation estimateMemoryUsageAfterLoading() {
        return getMemoryEstimation(storeConfig.nodeProjections(), storeConfig.relationshipProjections(), false);
    }

    private ProgressTracker initProgressTracker() {
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

        var task = Tasks.task(
            "Loading",
            Tasks.task("Nodes", Tasks.leaf("Store Scan", dimensions.nodeCount())),
            Tasks.task("Relationships", Tasks.leaf("Store Scan", relationshipCount))
        );

        if (graphProjectConfig.logProgress()) {
            return new TaskProgressTracker(
                task,
                loadingContext.log(),
                graphProjectConfig.readConcurrency(),
                graphProjectConfig.jobId(),
                loadingContext.taskRegistryFactory(),
                EmptyUserLogRegistryFactory.INSTANCE
            );
        }

        return new TaskTreeProgressTracker(
            task,
            loadingContext.log(),
            graphProjectConfig.readConcurrency(),
            graphProjectConfig.jobId(),
            loadingContext.taskRegistryFactory(),
            EmptyUserLogRegistryFactory.INSTANCE
        );
    }

    @Override
    public CSRGraphStore build() {
        validate(dimensions, storeConfig);

        var concurrency = graphProjectConfig.readConcurrency();
        try {
            progressTracker.beginSubTask();
            Nodes nodes = loadNodes(concurrency);
            RelationshipImportResult relationships = loadRelationships(nodes.idMap(), concurrency);
            CSRGraphStore graphStore = createGraphStore(nodes, relationships);

            logLoadingSummary(graphStore);

            return graphStore;
        } finally {
            progressTracker.endSubTask();
        }
    }

    private Nodes loadNodes(Concurrency concurrency) {
        var scanningNodesImporter = new ScanningNodesImporterBuilder()
            .concurrency(concurrency)
            .graphProjectConfig(graphProjectConfig)
            .dimensions(dimensions)
            .loadingContext(loadingContext)
            .progressTracker(progressTracker)
            .build();

        try {
            progressTracker.beginSubTask();
            return scanningNodesImporter.call();
        } finally {
            progressTracker.endSubTask();
        }
    }

    private RelationshipImportResult loadRelationships(IdMap idMap, Concurrency concurrency) {
        var scanningRelationshipsImporter = new ScanningRelationshipsImporterBuilder()
            .idMap(idMap)
            .graphProjectConfig(graphProjectConfig)
            .loadingContext(loadingContext)
            .dimensions(dimensions)
            .progressTracker(progressTracker)
            .concurrency(concurrency)
            .build();

        try {
            progressTracker.beginSubTask();
            return scanningRelationshipsImporter.call();
        } finally {
            progressTracker.endSubTask();
        }
    }

    @Override
    protected ProgressTracker progressTracker() {
        return progressTracker;
    }
}
