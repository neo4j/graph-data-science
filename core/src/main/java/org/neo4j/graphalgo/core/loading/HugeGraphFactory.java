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
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.GraphDimensionsValidation.validate;

public final class HugeGraphFactory extends GraphFactory {

    public HugeGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
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

    public static MemoryEstimation getMemoryEstimation(
        GraphDimensions dimensions
    ) {
        MemoryEstimations.Builder builder = MemoryEstimations
            .builder(HugeGraph.class)
            .add("nodeIdMap", IdMap.memoryEstimation());

        // Node properties
        for (ResolvedPropertyMapping resolvedPropertyMapping : dimensions.nodeProperties()) {
            builder.add(resolvedPropertyMapping.propertyKey(), NodePropertyMap.memoryEstimation());
        }

        // Relationships
        dimensions.relationshipProjectionMappings().stream().forEach(relationshipProjectionMapping -> {
            Optional<String> neoType = StringUtils.isBlank(relationshipProjectionMapping.typeName())
                ? Optional.empty()
                : Optional.of(relationshipProjectionMapping.typeName());
            String elementIdentifier = relationshipProjectionMapping.elementIdentifier();

            boolean undirected = relationshipProjectionMapping.projection() == Projection.UNDIRECTED;

            builder.add(
                "relationship with type " + elementIdentifier,
                AdjacencyList.compressedMemoryEstimation(neoType, undirected)
            );

            dimensions.relationshipProperties().mappings().forEach(resolvedPropertyMapping -> {
                builder.add(
                    "property " + resolvedPropertyMapping.propertyKey() + " for type " + neoType.orElse("<empty>"),
                    AdjacencyList.uncompressedMemoryEstimation(neoType, undirected)
                );
            });

            builder.add(
                "property offsets for type " + neoType.orElse("<empty>"),
                AdjacencyOffsets.memoryEstimation()
            );
        });


        return builder.build();
    }

    @Override
    protected ImportProgress importProgress(
        ProgressLogger progressLogger,
        GraphDimensions dimensions,
        GraphSetup setup
    ) {
        // batching for undirected double the amount of rels imported
        long relationshipCount = setup.relationshipProjections().projections().entrySet().stream().map(entry -> {
            Long relCount = dimensions.relationshipCounts().getOrDefault(entry.getKey().name, 0L);
            return entry.getValue().projection() == Projection.UNDIRECTED
                ? relCount * 2
                : relCount;
        }).mapToLong(Long::longValue).sum();

        return new ApproximatedImportProgress(
            progressLogger,
            setup.tracker(),
            dimensions.nodeCount(),
            relationshipCount
        );
    }

    @Override
    public ImportResult build() {
        validate(dimensions, setup);

        GraphDimensions dimensions = this.dimensions;
        int concurrency = setup.concurrency();
        AllocationTracker tracker = setup.tracker();
        IdsAndProperties mappingAndProperties = loadIdMap(tracker, concurrency);
        Map<String, Map<String, Graph>> graphs = loadRelationships(
                dimensions,
                tracker,
                mappingAndProperties,
                concurrency);
        progressLogger.logDone(tracker);

        return ImportResult.of(dimensions, GraphsByRelationshipType.of(graphs));
    }

    private IdsAndProperties loadIdMap(AllocationTracker tracker, int concurrency) {
        return new ScanningNodesImporter(
            api,
            dimensions,
            progress,
            tracker,
            setup.terminationFlag(),
            threadPool,
            concurrency,
            setup.nodePropertyMappings()
        ).call(setup.log());
    }

    private Map<String, Map<String, Graph>> loadRelationships(
            GraphDimensions dimensions,
            AllocationTracker tracker,
            IdsAndProperties idsAndProperties,
            int concurrency) {
        Map<RelationshipProjectionMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders = dimensions
                .relationshipProjectionMappings()
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        mapping -> createBuildersForRelationshipType(mapping, tracker)
                ));

        ScanningRelationshipsImporter scanningImporter = new ScanningRelationshipsImporter(
                setup,
                api,
                dimensions,
                progress,
                tracker,
                idsAndProperties.hugeIdMap,
                allBuilders,
                threadPool,
                concurrency
        );
        ObjectLongMap<RelationshipProjectionMapping> relationshipCounts = scanningImporter.call(setup.log());

        return allBuilders.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().elementIdentifier(),
                entry -> {
                    RelationshipProjectionMapping relProjectionMapping = entry.getKey();

                    Pair<RelationshipsBuilder, RelationshipsBuilder> builders = entry.getValue();
                    RelationshipsBuilder outgoingRelationshipsBuilder = builders.getOne();
                    RelationshipsBuilder incomingRelationshipsBuilder = builders.getTwo();

                    AdjacencyList outAdjacencyList = outgoingRelationshipsBuilder != null
                            ? outgoingRelationshipsBuilder.adjacencyListBuilder.build() : null;
                    AdjacencyOffsets outAdjacencyOffsets = outgoingRelationshipsBuilder != null
                            ? outgoingRelationshipsBuilder.globalAdjacencyOffsets : null;

                    AdjacencyList inAdjacencyList = incomingRelationshipsBuilder != null
                            ? incomingRelationshipsBuilder.adjacencyListBuilder.build() : null;
                    AdjacencyOffsets inAdjacencyOffsets = incomingRelationshipsBuilder != null
                            ? incomingRelationshipsBuilder.globalAdjacencyOffsets : null;

                    long relationshipCount = relationshipCounts.getOrDefault(relProjectionMapping, 0L);

                    // The factory loads at most one adjacency list for a single projection.
                    // We store that adjacency list always in the outgoing adjacency list.

                    if (relProjectionMapping.projection() == Projection.REVERSE) {
                        outAdjacencyList = inAdjacencyList;
                        outAdjacencyOffsets = inAdjacencyOffsets;
                        outgoingRelationshipsBuilder = incomingRelationshipsBuilder;
                    }

                    if (!dimensions.relationshipProperties().hasMappings()) {
                        HugeGraph graph = HugeGraph.create(
                                tracker,
                                idsAndProperties.hugeIdMap,
                                idsAndProperties.properties,
                                outAdjacencyList,
                                outAdjacencyOffsets,
                                relationshipCount,
                            setup.loadAsUndirected()
                        );
                        return Collections.singletonMap(ANY_REL_TYPE, graph);
                    } else {
                        AdjacencyList adjacencyList = outAdjacencyList;
                        AdjacencyOffsets adjacencyOffsets = outAdjacencyOffsets;
                        RelationshipsBuilder relationshipsBuilder = outgoingRelationshipsBuilder;

                        return dimensions.relationshipProperties().enumerate().map(propertyEntry -> {
                            int weightIndex = propertyEntry.getOne();
                            ResolvedPropertyMapping property = propertyEntry.getTwo();
                            HugeGraph graph = HugeGraph.create(
                                tracker,
                                idsAndProperties.hugeIdMap,
                                idsAndProperties.properties,
                                relationshipsBuilder,
                                adjacencyList,
                                adjacencyOffsets,
                                weightIndex,
                                property,
                                relationshipCount,
                                setup.loadAsUndirected()
                            );
                            return Tuples.pair(property.propertyKey(), graph);
                        }).collect(Collectors.toMap(Pair::getOne, Pair::getTwo));
                    }
                }));
    }

    private Pair<RelationshipsBuilder, RelationshipsBuilder> createBuildersForRelationshipType(
        RelationshipProjectionMapping relationshipProjectionMapping,
        AllocationTracker tracker
    ) {
        RelationshipsBuilder outgoingRelationshipsBuilder = null;
        RelationshipsBuilder incomingRelationshipsBuilder = null;

        Aggregation[] aggregations = dimensions
                .relationshipProperties()
                .stream()
                .map(property -> property.aggregation() == Aggregation.DEFAULT
                        ? Aggregation.NONE
                        : property.aggregation()
                )
                .toArray(Aggregation[]::new);
        // TODO: backwards compat code
        if (aggregations.length == 0) {
            Aggregation aggregation =
                setup.aggregation() == Aggregation.DEFAULT
                            ? Aggregation.NONE
                            : setup.aggregation();
            aggregations = new Aggregation[]{aggregation};
        }

        if (relationshipProjectionMapping.projection() == Projection.NATURAL || relationshipProjectionMapping.projection() == Projection.UNDIRECTED) {
            outgoingRelationshipsBuilder = new RelationshipsBuilder(
                aggregations,
                tracker,
                setup.relationshipPropertyMappings().numberOfMappings()
            );
        }
        if (relationshipProjectionMapping.projection() == Projection.REVERSE) {
            incomingRelationshipsBuilder = new RelationshipsBuilder(
                aggregations,
                tracker,
                setup.relationshipPropertyMappings().numberOfMappings()
            );
        }

        return Tuples.pair(outgoingRelationshipsBuilder, incomingRelationshipsBuilder);
    }

}
