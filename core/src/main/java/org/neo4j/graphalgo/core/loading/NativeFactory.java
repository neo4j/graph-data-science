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
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.core.Aggregation;
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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.GraphDimensionsValidation.validate;

public final class NativeFactory extends GraphStoreFactory {

    public NativeFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return memoryEstimation(dimensions);
    }

    @Override
    public MemoryEstimation memoryEstimation(GraphDimensions dimensions) {
        return getMemoryEstimation(dimensions);
    }

    public static MemoryEstimation getMemoryEstimation(GraphDimensions dimensions) {
        MemoryEstimations.Builder builder = MemoryEstimations
            .builder(HugeGraph.class)
            .add("nodeIdMap", IdMap.memoryEstimation());

        if (Objects.isNull(dimensions.relationshipProjectionMappings())) {
            throw new IllegalArgumentException("No relationship projection was specified.");
        }

        // node properties
        for (ResolvedPropertyMapping resolvedPropertyMapping : dimensions.nodeProperties()) {
            builder.add(resolvedPropertyMapping.propertyKey(), NodePropertyMap.memoryEstimation());
        }

        // relationships
        dimensions.relationshipProjectionMappings().stream().forEach(relationshipProjectionMapping -> {
            Optional<String> neoType = StringUtils.isBlank(relationshipProjectionMapping.typeName())
                ? Optional.empty()
                : Optional.of(relationshipProjectionMapping.typeName());
            String elementIdentifier = relationshipProjectionMapping.elementIdentifier();

            boolean undirected = relationshipProjectionMapping.orientation() == Orientation.UNDIRECTED;

            // adjacency list
            builder.add(
                String.format("adjacency list for '%s'", elementIdentifier),
                AdjacencyList.compressedMemoryEstimation(neoType, undirected)
            );
            builder.add(
                String.format("adjacency offsets for '%s'", elementIdentifier),
                AdjacencyOffsets.memoryEstimation()
            );
            // all properties per projection
            dimensions.relationshipProperties().mappings().forEach(resolvedPropertyMapping -> {
                builder.add(
                    String.format("property '%s.%s", elementIdentifier, resolvedPropertyMapping.propertyKey()),
                    AdjacencyList.uncompressedMemoryEstimation(neoType, undirected)
                );
                builder.add(
                    String.format("property offset '%s.%s", elementIdentifier, resolvedPropertyMapping.propertyKey()),
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
                     : dimensions.relationshipCounts().getOrDefault(entry.getKey().name, 0L);

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
        validate(dimensions, setup);

        int concurrency = setup.concurrency();
        AllocationTracker tracker = setup.tracker();
        IdsAndProperties nodes = loadNodes(tracker, concurrency);
        RelationshipImportResult relationships = loadRelationships(tracker, nodes, concurrency);
        GraphStore graphStore = createGraphStore(nodes, relationships, tracker, dimensions);
        progressLogger.logMessage(tracker);

        return ImportResult.of(dimensions, graphStore);
    }

    private IdsAndProperties loadNodes(AllocationTracker tracker, int concurrency) {
        return new ScanningNodesImporter(
            api,
            dimensions,
            progressLogger,
            tracker,
            setup.terminationFlag(),
            threadPool,
            concurrency,
            setup.nodePropertyMappings()
        ).call(setup.log());
    }

    private RelationshipImportResult loadRelationships(
        AllocationTracker tracker,
        IdsAndProperties idsAndProperties,
        int concurrency
    ) {
        Aggregation[] aggregations = dimensions.aggregations(setup.aggregation());
        int propertyCount = setup.relationshipPropertyMappings().numberOfMappings();
        Map<RelationshipProjectionMapping, RelationshipsBuilder> allBuilders = dimensions
            .relationshipProjectionMappings()
            .stream()
            .collect(Collectors.toMap(
                Function.identity(),
                mapping -> new RelationshipsBuilder(aggregations, tracker, propertyCount)
            ));

        ObjectLongMap<RelationshipProjectionMapping> relationshipCounts = new ScanningRelationshipsImporter(
            setup,
            api,
            dimensions,
            progressLogger,
            tracker,
            idsAndProperties.hugeIdMap,
            allBuilders,
            threadPool,
            concurrency
        ).call(setup.log());

        return RelationshipImportResult.of(allBuilders, relationshipCounts, dimensions);
    }
}
