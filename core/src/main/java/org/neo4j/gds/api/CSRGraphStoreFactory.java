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
package org.neo4j.gds.api;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseInfo.DatabaseLocation;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.AdjacencyBuffer;
import org.neo4j.gds.core.loading.AdjacencyListBehavior;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.Nodes;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class CSRGraphStoreFactory<CONFIG extends GraphProjectConfig> extends
    GraphStoreFactory<CSRGraphStore, CONFIG> {

    public CSRGraphStoreFactory(
        CONFIG graphProjectConfig,
        Capabilities capabilities,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions
    ) {
        super(graphProjectConfig, capabilities, loadingContext, dimensions);
    }

    protected CSRGraphStore createGraphStore(Nodes nodes, RelationshipImportResult relationshipImportResult) {
        var schema = MutableGraphSchema.of(
            nodes.schema(),
            relationshipImportResult.relationshipSchema(),
            Map.of()
        );

        return new GraphStoreBuilder()
            .databaseInfo(ImmutableDatabaseInfo.of(loadingContext.databaseId(), DatabaseLocation.LOCAL))
            .capabilities(capabilities)
            .schema(schema)
            .nodes(nodes)
            .relationshipImportResult(relationshipImportResult)
            .concurrency(graphProjectConfig.readConcurrency())
            .build();
    }

    protected void logLoadingSummary(GraphStore graphStore) {
        progressTracker().logDebug(() -> {
            var sizeInBytes = MemoryUsage.sizeOf(graphStore);
            if (sizeInBytes >= 0) {
                var memoryUsage = MemoryUsage.humanReadable(sizeInBytes);
                return formatWithLocale("Actual memory usage of the loaded graph: %s", memoryUsage);
            } else {
                return "Actual memory usage of the loaded graph could not be determined.";
            }
        });
    }

    protected abstract ProgressTracker progressTracker();

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
                builder.max(
                    List.of(
                        relationshipEstimationDuringLoading(relationshipType, relationshipProjection, undirected),
                        relationshipEstimationAfterLoading(relationshipType, relationshipProjection, undirected)
                    )
                );
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
                relationshipType,
                (int) relationshipProjection.properties().stream().count(),
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
            .forEach(
                resolvedPropertyMapping -> estimationBuilder.perNode(
                    formatWithLocale(
                        "property '%s.%s'%s",
                        relationshipType,
                        resolvedPropertyMapping.propertyKey(),
                        indexSuffix
                    ),
                    HugeLongArray::memoryEstimation
                )
            );
    }

    private static MemoryEstimation relationshipEstimationAfterLoading(
        RelationshipType relationshipType,
        RelationshipProjection relationshipProjection,
        boolean undirected
    ) {
        var afterLoadingEstimation = MemoryEstimations.builder("size after loading");

        CSRGraphStoreFactory.relationshipEstimationAfterLoading(
            relationshipType,
            relationshipProjection,
            undirected,
            false,
            afterLoadingEstimation
        );
        if (relationshipProjection.indexInverse()) {
            CSRGraphStoreFactory.relationshipEstimationAfterLoading(
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
}
