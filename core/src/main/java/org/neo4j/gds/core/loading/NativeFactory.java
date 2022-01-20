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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.GraphDimensionsStoreReader;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.compress.AdjacencyFactory;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.internal.id.IdGeneratorFactory;

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.core.GraphDimensionsValidation.validate;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NativeFactory extends CSRGraphStoreFactory<GraphProjectFromStoreConfig> {

    private final GraphProjectFromStoreConfig storeConfig;

    public NativeFactory(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext
    ) {
        this(
            graphProjectConfig,
            loadingContext,
            new GraphDimensionsStoreReader(
                loadingContext.transactionContext(),
                graphProjectConfig,
                GraphDatabaseApiProxy.resolveDependency(loadingContext.api(), IdGeneratorFactory.class)
            ).call()
        );
    }

    public NativeFactory(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions graphDimensions
    ) {
        super(graphProjectConfig, loadingContext, graphDimensions);
        this.storeConfig = graphProjectConfig;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return getMemoryEstimation(storeConfig.nodeProjections(), storeConfig.relationshipProjections());
    }

    public static MemoryEstimation getMemoryEstimation(
        NodeProjections nodeProjections,
        RelationshipProjections relationshipProjections
    ) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(HugeGraph.class);

        // node information
        builder.add("nodeIdMap", IdMapBehaviorServiceProvider.idMapBehavior().memoryEstimation());

        // nodeProperties
        nodeProjections.allProperties()
            .forEach(property -> builder.add(property, NodePropertiesFromStoreBuilder.memoryEstimation()));

        // relationships
        relationshipProjections.projections().forEach((relationshipType, relationshipProjection) -> {

            boolean undirected = relationshipProjection.orientation() == Orientation.UNDIRECTED;

            // adjacency list
            builder.add(
                formatWithLocale("adjacency list for '%s'", relationshipType),
                AdjacencyFactory.adjacencyListEstimation(relationshipType, undirected)
            );
            // all properties per projection
            relationshipProjection.properties().mappings().forEach(resolvedPropertyMapping -> {
                builder.add(
                    formatWithLocale("property '%s.%s", relationshipType, resolvedPropertyMapping.propertyKey()),
                    AdjacencyFactory.adjacencyPropertiesEstimation(relationshipType, undirected)
                );
            });
        });

        return builder.build();
    }

    @Override
    protected ProgressTracker initProgressTracker() {
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

        var properties = IndexPropertyMappings.prepareProperties(
            graphProjectConfig,
            dimensions,
            loadingContext.transactionContext()
        );

        List<Task> nodeTasks = properties.indexedProperties().isEmpty()
            ? List.of(Tasks.leaf("Store Scan", dimensions.nodeCount()))
            : List.of(
                Tasks.leaf("Store Scan", dimensions.nodeCount()),
                Tasks.leaf("Property Index Scan", properties.indexedProperties().size() * dimensions.nodeCount())
            );

        var task = Tasks.task(
            "Loading",
            Tasks.task("Nodes", nodeTasks),
            Tasks.task("Relationships", Tasks.leaf("Store Scan", relationshipCount))
        );
        return new TaskProgressTracker(
            task,
            loadingContext.log(),
            graphProjectConfig.readConcurrency(),
            loadingContext.taskRegistryFactory()
        );
    }

    @Override
    public CSRGraphStore build() {
        validate(dimensions, storeConfig);

        int concurrency = graphProjectConfig.readConcurrency();
        AllocationTracker allocationTracker = loadingContext.allocationTracker();
        try {
            progressTracker.beginSubTask();
            IdMapAndProperties nodes = loadNodes(concurrency);
            RelationshipsAndProperties relationships = loadRelationships(nodes.idMap(), concurrency);
            CSRGraphStore graphStore = createGraphStore(nodes, relationships, allocationTracker);

            logLoadingSummary(graphStore, Optional.of(allocationTracker));

            return graphStore;
        } finally {
            progressTracker.endSubTask();
        }
    }

    private IdMapAndProperties loadNodes(int concurrency) {
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

    private RelationshipsAndProperties loadRelationships(IdMap idMap, int concurrency) {
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
}
