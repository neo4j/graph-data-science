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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.GraphDimensionsStoreReader;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.compress.AdjacencyListBehavior;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.internal.id.IdGeneratorFactory;

import java.util.List;

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
                GraphDatabaseApiProxy.resolveDependency(loadingContext.graphDatabaseService(), IdGeneratorFactory.class)
            ).call()
        );
    }

    public NativeFactory(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions graphDimensions
    ) {
        super(graphProjectConfig, ImmutableStaticCapabilities.of(true), loadingContext, graphDimensions);
        this.storeConfig = graphProjectConfig;
    }

    @Override
    public MemoryEstimation estimateMemoryUsageDuringLoading() {
        return getMemoryEstimation(storeConfig.nodeProjections(), storeConfig.relationshipProjections(), true);
    }

    @Override
    public MemoryEstimation estimateMemoryUsageAfterLoading() {
        return getMemoryEstimation(storeConfig.nodeProjections(), storeConfig.relationshipProjections(), false);
    }

    public static MemoryEstimation getMemoryEstimation(
        NodeProjections nodeProjections,
        RelationshipProjections relationshipProjections,
        boolean isLoading
    ) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder("graph projection");

        // node information
        builder.add("nodeIdMap", IdMapBehaviorServiceProvider.idMapBehavior().memoryEstimation());

        // nodeProperties
        nodeProjections.allProperties()
            .forEach(property -> builder.add(property, NodePropertiesFromStoreBuilder.memoryEstimation()));

        // relationships
        relationshipProjections.projections().forEach((relationshipType, relationshipProjection) -> {
            boolean undirected = relationshipProjection.orientation() == Orientation.UNDIRECTED;
            if (isLoading) {
                builder.max(List.of(
                    relationshipEstimationDuringLoading(relationshipType, relationshipProjection, undirected),
                    relationshipEstimationAfterLoading(relationshipType, relationshipProjection, undirected)
                ));
            } else {
                builder.add(MemoryEstimations.builder(HugeGraph.class).build());
                builder.add(relationshipEstimationAfterLoading(relationshipType, relationshipProjection, undirected));
            }
        });

        return builder.build();
    }

    @NotNull
    private static MemoryEstimation relationshipEstimationDuringLoading(
        RelationshipType relationshipType,
        RelationshipProjection relationshipProjection,
        boolean undirected
    ) {
        var duringLoadingEstimation = MemoryEstimations.builder("size during loading");

        relationshipEstimationDuringLoading(
            relationshipType,
            relationshipProjection,
            undirected,
            false,
            duringLoadingEstimation
        );

        if (relationshipProjection.indexInverse()) {
            relationshipEstimationDuringLoading(
                relationshipType,
                relationshipProjection,
                undirected,
                true,
                duringLoadingEstimation
            );
        }

        return duringLoadingEstimation.build();
    }

    private static void relationshipEstimationDuringLoading(
        RelationshipType relationshipType,
        RelationshipProjection relationshipProjection,
        boolean undirected,
        boolean printIndexSuffix,
        MemoryEstimations.Builder estimationBuilder
    ) {
        var indexSuffix = printIndexSuffix ? " (inverse index)" : "";

        estimationBuilder.add(
            formatWithLocale(
                "adjacency loading buffer for '%s'%s",
                relationshipType,
                indexSuffix
            ),
            AdjacencyBuffer.memoryEstimation(
                relationshipType, (int) relationshipProjection.properties().stream().count(),
                undirected
            )
        );

        // Offsets and degrees are eagerly initialized and exist next to the fully populated AdjacencyBuffer
        estimationBuilder.perNode(
            formatWithLocale("offsets for '%s'%s", relationshipType, indexSuffix),
            HugeLongArray::memoryEstimation
        );
        estimationBuilder.perNode(
            formatWithLocale("degrees for '%s'%s", relationshipType, indexSuffix),
            HugeIntArray::memoryEstimation
        );
        relationshipProjection
            .properties()
            .mappings()
            .forEach(resolvedPropertyMapping -> estimationBuilder.perNode(
                formatWithLocale(
                    "property '%s.%s'%s",
                    relationshipType,
                    resolvedPropertyMapping.propertyKey(),
                    indexSuffix
                ),
                HugeLongArray::memoryEstimation
            ));
    }

    private static MemoryEstimation relationshipEstimationAfterLoading(
        RelationshipType relationshipType,
        RelationshipProjection relationshipProjection,
        boolean undirected
    ) {
        var afterLoadingEstimation = MemoryEstimations.builder("size after loading");

        relationshipEstimationAfterLoading(
            relationshipType,
            relationshipProjection,
            undirected,
            false,
            afterLoadingEstimation
        );
        if (relationshipProjection.indexInverse()) {
            relationshipEstimationAfterLoading(
                relationshipType,
                relationshipProjection,
                undirected,
                true,
                afterLoadingEstimation
            );
        }

        return afterLoadingEstimation.build();
    }

    private static void relationshipEstimationAfterLoading(
        RelationshipType relationshipType,
        RelationshipProjection relationshipProjection,
        boolean undirected,
        boolean printIndexSuffix,
        MemoryEstimations.Builder afterLoadingEstimation
    ) {
        var indexSuffix = printIndexSuffix ? " (inverse index)" : "";

        // adjacency list
        afterLoadingEstimation.add(
            formatWithLocale("adjacency list for '%s'%s", relationshipType, indexSuffix),
            AdjacencyListBehavior.adjacencyListEstimation(relationshipType, undirected)
        );
        // all properties per projection
        relationshipProjection.properties().mappings().forEach(resolvedPropertyMapping -> {
            afterLoadingEstimation.add(
                formatWithLocale(
                    "property '%s.%s%s",
                    relationshipType,
                    resolvedPropertyMapping.propertyKey(),
                    indexSuffix
                ),
                AdjacencyListBehavior.adjacencyPropertiesEstimation(relationshipType, undirected)
            );
        });
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
            graphProjectConfig.jobId(),
            loadingContext.taskRegistryFactory(),
            EmptyUserLogRegistryFactory.INSTANCE
        );
    }

    @Override
    protected MutableGraphSchema computeGraphSchema(
        Nodes nodes, RelationshipImportResult relationshipImportResult
    ) {
        return CSRGraphStoreUtil.computeGraphSchema(
            nodes,
            relationshipImportResult
        );
    }

    @Override
    public CSRGraphStore build() {
        validate(dimensions, storeConfig);

        int concurrency = graphProjectConfig.readConcurrency();
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

    private Nodes loadNodes(int concurrency) {
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

    private RelationshipImportResult loadRelationships(IdMap idMap, int concurrency) {
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
