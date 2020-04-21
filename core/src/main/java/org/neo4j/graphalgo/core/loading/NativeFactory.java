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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.GraphDimensionsValidation.validate;

public final class NativeFactory extends GraphStoreFactory {

    public NativeFactory(GraphDatabaseAPI api, GraphCreateConfig graphCreateConfig, GraphSetup setup) {
        super(api, setup, graphCreateConfig);
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return memoryEstimation(dimensions);
    }

    @Override
    public MemoryEstimation memoryEstimation(GraphDimensions dimensions) {
        return getMemoryEstimation(dimensions, graphCreateConfig);
    }

    public static MemoryEstimation getMemoryEstimation(GraphDimensions dimensions, RelationshipProjections relationshipProjections) {
        GraphCreateFromStoreConfig config = GraphCreateFromStoreConfig.of(
            "",
            "",
            NodeProjections.of(),
            relationshipProjections,
            CypherMapWrapper.empty()
        );
        return getMemoryEstimation(dimensions, config);
    }

    public static MemoryEstimation getMemoryEstimation(GraphDimensions dimensions, GraphCreateConfig config) {
        MemoryEstimations.Builder builder = MemoryEstimations
            .builder(HugeGraph.class)
            .add("nodeIdMap", IdMap.memoryEstimation());

        // nodes
        dimensions
            .nodePropertyTokens()
            .keySet()
            .forEach(property -> builder.add(property, NodePropertyMap.memoryEstimation()));

        // relationships
        config.relationshipProjections().projections().forEach((relationshipType, relationshipProjection) -> {

            boolean undirected = relationshipProjection.orientation() == Orientation.UNDIRECTED;

            // adjacency list
            builder.add(
                String.format("adjacency list for '%s'", relationshipType),
                AdjacencyList.compressedMemoryEstimation(relationshipType, undirected)
            );
            builder.add(
                String.format("adjacency offsets for '%s'", relationshipType),
                AdjacencyOffsets.memoryEstimation()
            );
            // all properties per projection
            relationshipProjection.properties().mappings().forEach(resolvedPropertyMapping -> {
                builder.add(
                    String.format("property '%s.%s", relationshipType, resolvedPropertyMapping.propertyKey()),
                    AdjacencyList.uncompressedMemoryEstimation(relationshipType, undirected)
                );
                builder.add(
                    String.format("property offset '%s.%s", relationshipType, resolvedPropertyMapping.propertyKey()),
                    AdjacencyOffsets.memoryEstimation()
                );
            });
        });

        return builder.build();
    }

    @Override
    protected ProgressLogger initProgressLogger() {
        long relationshipCount = setup.relationshipProjections().projections().entrySet().stream()
            .map(entry -> {
                Long relCount = entry.getKey().name.equals("*")
                     ? dimensions.relationshipCounts().values().stream().reduce(Long::sum).orElse(0L)
                     : dimensions.relationshipCounts().getOrDefault(entry.getKey(), 0L);

                return entry.getValue().orientation() == Orientation.UNDIRECTED
                    ? relCount * 2
                    : relCount;
            }).mapToLong(Long::longValue).sum();

        return new BatchingProgressLogger(
            log,
            dimensions.nodeCount() + relationshipCount,
            TASK_LOADING
        );
    }

    @Override
    public ImportResult build() {
        validate(dimensions, graphCreateConfig);

        int concurrency = setup.concurrency();
        AllocationTracker tracker = setup.tracker();
        IdsAndProperties nodes = loadNodes(tracker, concurrency);
        RelationshipImportResult relationships = loadRelationships(tracker, nodes, concurrency);
        GraphStore graphStore = createGraphStore(nodes, relationships, tracker, dimensions);
        progressLogger.logMessage(tracker);

        return ImportResult.of(dimensions, graphStore);
    }

    private IdsAndProperties loadNodes(AllocationTracker tracker, int concurrency) {
        Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel = graphCreateConfig
            .nodeProjections()
            .projections()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().properties()
            ));

        return new ScanningNodesImporter(
            api,
            dimensions,
            progressLogger,
            tracker,
            setup.terminationFlag(),
            threadPool,
            concurrency,
            propertyMappingsByNodeLabel
        ).call(setup.log());
    }

    private RelationshipImportResult loadRelationships(
        AllocationTracker tracker,
        IdsAndProperties idsAndProperties,
        int concurrency
    ) {
        Map<RelationshipType, RelationshipsBuilder> allBuilders = graphCreateConfig
            .relationshipProjections()
            .projections()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                projectionEntry -> new RelationshipsBuilder(projectionEntry.getValue(), tracker)
            ));

        Map<RelationshipType, Integer> relationshipTypeNeoTypeIdMapping = new HashMap<>(dimensions.typeTokenRelationshipTypeMapping().size());
        dimensions.typeTokenRelationshipTypeMapping().forEach((Consumer<? super IntObjectCursor<List<RelationshipType>>>) cursor -> {
            int typeId = cursor.key;
            List<RelationshipType> relationshipTypes = cursor.value;
            relationshipTypes.forEach(relationshipType -> relationshipTypeNeoTypeIdMapping.put(relationshipType, typeId));
        });

        ObjectLongMap<RelationshipType> relationshipCounts = new ScanningRelationshipsImporter(
            setup,
            api,
            dimensions,
            progressLogger,
            tracker,
            idsAndProperties.idMap,
            allBuilders,
            relationshipTypeNeoTypeIdMapping,
            threadPool,
            concurrency
        ).call(setup.log());

        return RelationshipImportResult.of(allBuilders, relationshipCounts, dimensions);
    }
}
