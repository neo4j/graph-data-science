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

import com.carrotsearch.hppc.ObjectLongMap;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.GraphDimensionsStoreReader;
import org.neo4j.gds.core.IdMapBehaviorServiceLoader;
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
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.core.GraphDimensionsValidation.validate;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NativeFactory extends CSRGraphStoreFactory<GraphCreateFromStoreConfig> {

    private final GraphCreateFromStoreConfig storeConfig;

    public NativeFactory(
        GraphCreateFromStoreConfig graphCreateConfig,
        GraphLoaderContext loadingContext
    ) {
        this(
            graphCreateConfig,
            loadingContext,
            new GraphDimensionsStoreReader(
                loadingContext.transactionContext(),
                graphCreateConfig,
                GraphDatabaseApiProxy.resolveDependency(loadingContext.api(), IdGeneratorFactory.class)
            ).call()
        );
    }

    public NativeFactory(
        GraphCreateFromStoreConfig graphCreateConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions graphDimensions
    ) {
        super(graphCreateConfig, loadingContext, graphDimensions);
        this.storeConfig = graphCreateConfig;
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
        builder.add("nodeIdMap", IdMapBehaviorServiceLoader.INSTANCE.memoryEstimation());

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
        long relationshipCount = graphCreateConfig
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
            graphCreateConfig,
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
            graphCreateConfig.readConcurrency(),
            loadingContext.taskRegistryFactory()
        );
    }

    @Override
    public ImportResult<CSRGraphStore> build() {
        validate(dimensions, storeConfig);

        int concurrency = graphCreateConfig.readConcurrency();
        AllocationTracker allocationTracker = loadingContext.allocationTracker();
        try {
            progressTracker.beginSubTask();
            IdsAndProperties nodes = loadNodes(concurrency);
            RelationshipImportResult relationships = loadRelationships(allocationTracker, nodes, concurrency);
            CSRGraphStore graphStore = createGraphStore(nodes, relationships, allocationTracker, dimensions);

            logLoadingSummary(graphStore, Optional.of(allocationTracker));

            return ImportResult.of(dimensions, graphStore);
        } finally {
            progressTracker.endSubTask();
        }
    }

    private IdsAndProperties loadNodes(int concurrency) {
        var properties = IndexPropertyMappings.prepareProperties(
            graphCreateConfig,
            dimensions,
            loadingContext.transactionContext()
        );

        var idMapBehavior = IdMapBehaviorServiceLoader.INSTANCE;

        var pair = idMapBehavior.create(true, loadingContext.allocationTracker());

        var scanningNodesImporter = new ScanningNodesImporter<>(
            graphCreateConfig,
            loadingContext,
            dimensions,
            progressTracker,
            loadingContext.log(),
            concurrency,
            properties,
            pair.getLeft(),
            pair.getRight()
        );

        try {
            progressTracker.beginSubTask();
            return scanningNodesImporter.call();
        } finally {
            progressTracker.endSubTask();
        }
    }

    private RelationshipImportResult loadRelationships(
        AllocationTracker allocationTracker,
        IdsAndProperties idsAndProperties,
        int concurrency
    ) {
        Map<RelationshipType, AdjacencyListWithPropertiesBuilder> allBuilders = graphCreateConfig
            .relationshipProjections()
            .projections()
            .entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                projectionEntry -> AdjacencyListWithPropertiesBuilder.create(
                    dimensions::nodeCount,
                    AdjacencyFactory.configured(),
                    projectionEntry.getValue(),
                    dimensions.relationshipPropertyTokens(),
                    allocationTracker
                )
            ));

        ScanningRelationshipsImporter scanningRelationshipsImporter = new ScanningRelationshipsImporter(
            graphCreateConfig,
            loadingContext,
            dimensions,
            progressTracker,
            idsAndProperties.idMap(),
            allBuilders,
            concurrency
        );

        try {
            progressTracker.beginSubTask();

            ObjectLongMap<RelationshipType> relationshipCounts = scanningRelationshipsImporter.call();

            return RelationshipImportResult.of(allBuilders, relationshipCounts, dimensions);
        } finally {
            progressTracker.endSubTask();
        }
    }
}
