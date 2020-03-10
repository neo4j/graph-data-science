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
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphDimensionsReader;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.ImmutableTopologyCSR;
import org.neo4j.graphalgo.core.huge.ImmutablePropertyCSR;
import org.neo4j.graphalgo.core.loading.ApproximatedImportProgress;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.loading.ImportProgress;
import org.neo4j.graphalgo.core.loading.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.Assessable;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The Abstract Factory defines the construction of the graph
 */
public abstract class GraphStoreFactory implements Assessable {

    public static final String TASK_LOADING = "LOADING";

    protected final ExecutorService threadPool;
    protected final GraphDatabaseAPI api;
    protected final GraphSetup setup;
    protected final GraphDimensions dimensions;
    protected final ImportProgress progress;
    protected final Log log;
    protected final ProgressLogger progressLogger;

    public GraphStoreFactory(GraphDatabaseAPI api, GraphSetup setup) {
        this(api, setup, true);
    }

    public GraphStoreFactory(GraphDatabaseAPI api, GraphSetup setup, boolean readTokens) {
        this.threadPool = setup.executor();
        this.api = api;
        this.setup = setup;
        this.log = setup.log();
        this.progressLogger = progressLogger(log, setup.logMillis());
        this.dimensions = new GraphDimensionsReader(api, setup, readTokens).call();
        this.progress = importProgress(progressLogger, dimensions, setup);
    }

    public abstract ImportResult build();

    public abstract MemoryEstimation memoryEstimation(GraphDimensions dimensions);

    public GraphDimensions dimensions() {
        return this.dimensions;
    }

    protected ImportProgress importProgress(
        ProgressLogger progressLogger,
        GraphDimensions dimensions,
        GraphSetup setup
    ) {
        long relationshipCount = dimensions.relationshipProjectionMappings().stream()
            .mapToLong(mapping -> {
                Long typeCount = dimensions.relationshipCounts().get(mapping.elementIdentifier());
                return mapping.orientation() == Orientation.UNDIRECTED
                    ? typeCount * 2
                    : typeCount;
            })
            .sum();

        return new ApproximatedImportProgress(
            progressLogger,
            setup.tracker(),
            dimensions.nodeCount(),
            relationshipCount
        );
    }

    protected GraphStore createGraphStore(
        IdsAndProperties idsAndProperties,
        RelationshipImportResult relationshipImportResult,
        AllocationTracker tracker,
        GraphDimensions dimensions
    ) {
        int relTypeCount = dimensions.relationshipProjectionMappings().numberOfMappings();
        Map<String, HugeGraph.TopologyCSR> relationships = new HashMap<>(relTypeCount);
        Map<String, Map<String, HugeGraph.PropertyCSR>> relationshipProperties = new HashMap<>(relTypeCount);

        relationshipImportResult.builders().forEach((relationshipProjectionMapping, relationshipsBuilder) -> {
            AdjacencyList adjacencyList = relationshipsBuilder.adjacencyList();
            AdjacencyOffsets adjacencyOffsets = relationshipsBuilder.globalAdjacencyOffsets();
            long relationshipCount = relationshipImportResult.counts().getOrDefault(relationshipProjectionMapping, 0L);

            relationships.put(
                relationshipProjectionMapping.elementIdentifier(),
                ImmutableTopologyCSR.of(
                    adjacencyList,
                    adjacencyOffsets,
                    relationshipCount,
                    relationshipProjectionMapping.orientation()
                )
            );

            if (dimensions.relationshipProperties().hasMappings()) {
                Map<String, HugeGraph.PropertyCSR> propertyMap = dimensions
                    .relationshipProperties()
                    .enumerate()
                    .filter(propertyIdAndMapping -> propertyIdAndMapping.getTwo().exists())
                    .collect(Collectors.toMap(
                        propertyIdAndMapping -> propertyIdAndMapping.getTwo().propertyKey(),
                        propertyIdAndMapping -> ImmutablePropertyCSR.of(
                            relationshipsBuilder.properties(propertyIdAndMapping.getOne()),
                            relationshipsBuilder.globalPropertyOffsets(propertyIdAndMapping.getOne()),
                            relationshipCount,
                            relationshipProjectionMapping.orientation(),
                            propertyIdAndMapping.getTwo().defaultValue()
                        )
                    ));
                relationshipProperties.put(relationshipProjectionMapping.elementIdentifier(), propertyMap);
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

    private static ProgressLogger progressLogger(Log log, long time) {
        return ProgressLogger.wrap(log, TASK_LOADING, time, TimeUnit.MILLISECONDS);
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
        Map<RelationshipProjectionMapping, RelationshipsBuilder> builders();

        ObjectLongMap<RelationshipProjectionMapping> counts();

        GraphDimensions dimensions();

        static RelationshipImportResult of(
            Map<RelationshipProjectionMapping, RelationshipsBuilder> builders,
            ObjectLongMap<RelationshipProjectionMapping> counts,
            GraphDimensions dimensions
        ) {
            return ImmutableRelationshipImportResult.of(builders, counts, dimensions);
        }
    }
}
