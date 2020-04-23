/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.api;

import com.carrotsearch.hppc.ObjectLongMap;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphDimensionsReader;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.ImmutablePropertyCSR;
import org.neo4j.graphalgo.core.huge.ImmutableTopologyCSR;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.loading.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.Assessable;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * The Abstract Factory defines the construction of the graph
 */
public abstract class GraphStoreFactory implements Assessable {

    public static final String TASK_LOADING = "LOADING";

    protected final ExecutorService threadPool;
    protected final GraphLoadingContext loadingContext;
    protected final GraphDimensions dimensions;
    protected final ProgressLogger progressLogger;
    protected final GraphCreateConfig graphCreateConfig;

    public GraphStoreFactory(
        GraphLoadingContext loadingContext,
        GraphCreateConfig graphCreateConfig
    ) {
        this(loadingContext, graphCreateConfig, true);
    }

    public GraphStoreFactory(
        GraphLoadingContext loadingContext,
        GraphCreateConfig graphCreateConfig,
        boolean readTokens
    ) {
        this.threadPool = loadingContext.executor();
        this.loadingContext = loadingContext;
        this.graphCreateConfig = graphCreateConfig;
        this.dimensions = new GraphDimensionsReader(loadingContext.api(), graphCreateConfig, readTokens).call();
        this.progressLogger = initProgressLogger();
    }

    public abstract ImportResult build();

    public abstract MemoryEstimation memoryEstimation(GraphDimensions dimensions);

    public GraphDimensions dimensions() {
        return this.dimensions;
    }

    protected ProgressLogger initProgressLogger() {
        long relationshipCount = graphCreateConfig
            .relationshipProjections()
            .projections()
            .entrySet()
            .stream()
            .mapToLong(projectionEntry -> {
                Long typeCount = dimensions.relationshipCounts().getOrDefault(projectionEntry.getKey(), 0L);
                return projectionEntry.getValue().orientation() == Orientation.UNDIRECTED
                    ? typeCount * 2
                    : typeCount;
            })
            .sum();

        return new BatchingProgressLogger(
            loadingContext.log(),
            dimensions.nodeCount() + relationshipCount,
            TASK_LOADING,
            graphCreateConfig.readConcurrency()
        );
    }

    protected GraphStore createGraphStore(
        IdsAndProperties idsAndProperties,
        RelationshipImportResult relationshipImportResult,
        AllocationTracker tracker,
        GraphDimensions dimensions
    ) {
        int relTypeCount = dimensions.relationshipTypeTokens().size();
        Map<RelationshipType, HugeGraph.TopologyCSR> relationships = new HashMap<>(relTypeCount);
        Map<RelationshipType, Map<String, HugeGraph.PropertyCSR>> relationshipProperties = new HashMap<>(relTypeCount);

        relationshipImportResult.builders().forEach((relationshipType, relationshipsBuilder) -> {
            AdjacencyList adjacencyList = relationshipsBuilder.adjacencyList();
            AdjacencyOffsets adjacencyOffsets = relationshipsBuilder.globalAdjacencyOffsets();
            long relationshipCount = relationshipImportResult.counts().getOrDefault(relationshipType, 0L);

            RelationshipProjection projection = relationshipsBuilder.projection();

            relationships.put(
                relationshipType,
                ImmutableTopologyCSR.of(
                    adjacencyList,
                    adjacencyOffsets,
                    relationshipCount,
                    projection.orientation()
                )
            );

            PropertyMappings propertyMappings = projection.properties();
            if (!propertyMappings.isEmpty()) {
                Map<String, HugeGraph.PropertyCSR> propertyMap = propertyMappings
                    .enumerate()
                    .collect(Collectors.toMap(
                        propertyIndexAndMapping -> propertyIndexAndMapping.getTwo().propertyKey(),
                        propertyIndexAndMapping -> ImmutablePropertyCSR.of(
                            relationshipsBuilder.properties(propertyIndexAndMapping.getOne()),
                            relationshipsBuilder.globalPropertyOffsets(propertyIndexAndMapping.getOne()),
                            relationshipCount,
                            projection.orientation(),
                            propertyIndexAndMapping.getTwo().defaultValue()
                        )
                    ));
                relationshipProperties.put(relationshipType, propertyMap);
            }
        });

        return GraphStore.of(
            idsAndProperties.idMap(),
            idsAndProperties.properties(),
            relationships,
            relationshipProperties,
            tracker
        );
    }

    @ValueClass
    public interface ImportResult {
        GraphDimensions dimensions();

        GraphStore graphStore();

        static ImportResult of(GraphDimensions dimensions, GraphStore graphStore) {
            return ImmutableImportResult.builder()
                .dimensions(dimensions)
                .graphStore(graphStore)
                .build();
        }
    }

    @ValueClass
    public interface RelationshipImportResult {
        Map<RelationshipType, RelationshipsBuilder> builders();

        ObjectLongMap<RelationshipType> counts();

        GraphDimensions dimensions();

        static RelationshipImportResult of(
            Map<RelationshipType, RelationshipsBuilder> builders,
            ObjectLongMap<RelationshipType> counts,
            GraphDimensions dimensions
        ) {
            return ImmutableRelationshipImportResult.of(builders, counts, dimensions);
        }
    }
}
