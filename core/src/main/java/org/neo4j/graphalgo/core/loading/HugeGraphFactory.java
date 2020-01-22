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
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.GraphDimensionsValidation.validate;

public final class HugeGraphFactory extends GraphFactory {

    // TODO: make this configurable from somewhere
    private static final boolean LOAD_DEGREES = false;

    public HugeGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return memoryEstimation(setup, dimensions);
    }

    @Override
    public MemoryEstimation memoryEstimation(GraphSetup setup, GraphDimensions dimensions) {
        return getMemoryEstimation(setup, dimensions);
    }

    public static MemoryEstimation getMemoryEstimation(GraphSetup setup, GraphDimensions dimensions) {
        return getMemoryEstimation(setup.loadOutgoing(), setup.loadIncoming(), setup.loadAsUndirected(), dimensions);
    }

    public static MemoryEstimation getMemoryEstimation(
        boolean loadOutgoing,
        boolean loadIncoming,
        boolean loadAsUndirected,
        GraphDimensions dimensions
    ) {
        MemoryEstimations.Builder builder = MemoryEstimations
            .builder(HugeGraph.class)
            .add("nodeIdMap", IdMap.memoryEstimation());

        // Node properties
        for (ResolvedPropertyMapping resolvedPropertyMapping : dimensions.nodeProperties()) {
            if (resolvedPropertyMapping.exists()) {
                builder.add(resolvedPropertyMapping.propertyKey(), NodePropertyMap.memoryEstimation());
            } else {
                builder.add(resolvedPropertyMapping.propertyKey(), NullPropertyMap.MEMORY_USAGE);
            }
        }

        // Relationship properties
        for (ResolvedPropertyMapping mapping : dimensions.relationshipProperties()) {
            if (mapping.exists()) {
                // Adjacency lists and Adjacency offsets
                MemoryEstimation adjacencyListSize = AdjacencyList.uncompressedMemoryEstimation(loadAsUndirected);
                MemoryEstimation adjacencyOffsetsSetup = AdjacencyOffsets.memoryEstimation();
                if (loadOutgoing || loadAsUndirected) {
                    builder.add("outgoing properties for " + mapping.neoPropertyKey(), adjacencyListSize);
                    builder.add("outgoing property offsets for " + mapping.neoPropertyKey(), adjacencyOffsetsSetup);

                }
                if (loadIncoming && !loadAsUndirected) {
                    builder.add("incoming properties for " + mapping.neoPropertyKey(), adjacencyListSize);
                    builder.add("incoming property offsets for " + mapping.neoPropertyKey(), adjacencyOffsetsSetup);
                }
            }
        }

        // Adjacency lists and Adjacency offsets
        MemoryEstimation adjacencyListSize = AdjacencyList.compressedMemoryEstimation(loadAsUndirected);
        MemoryEstimation adjacencyOffsetsSetup = AdjacencyOffsets.memoryEstimation();
        if (loadOutgoing || loadAsUndirected) {
            builder.add("outgoing", adjacencyListSize);
            builder.add("outgoing offsets", adjacencyOffsetsSetup);

        }
        if (loadIncoming && !loadAsUndirected) {
            builder.add("incoming", adjacencyListSize);
            builder.add("incoming offsets", adjacencyOffsetsSetup);
        }

        return builder.build();
    }

    @Override
    protected ImportProgress importProgress(
            ProgressLogger progressLogger,
            GraphDimensions dimensions,
            GraphSetup setup) {

        // ops for scanning degrees
        long relOperations = LOAD_DEGREES ? dimensions.maxRelCount() : 0L;

        // batching for undirected double the amount of rels imported
        if (setup.loadIncoming() || setup.loadAsUndirected()) {
            relOperations += dimensions.maxRelCount();
        }
        if (setup.loadOutgoing() || setup.loadAsUndirected()) {
            relOperations += dimensions.maxRelCount();
        }

        return new ApproximatedImportProgress(
                progressLogger,
            setup.tracker(),
                dimensions.nodeCount(),
                relOperations
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
        )
                .call(setup.log());
    }

    private Map<String, Map<String, Graph>> loadRelationships(
            GraphDimensions dimensions,
            AllocationTracker tracker,
            IdsAndProperties idsAndProperties,
            int concurrency) {
        Map<RelationshipTypeMapping, Pair<RelationshipsBuilder, RelationshipsBuilder>> allBuilders = dimensions
                .relationshipTypeMappings()
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        mapping -> createBuildersForRelationshipType(tracker)
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
        ObjectLongMap<RelationshipTypeMapping> relationshipCounts = scanningImporter.call(setup.log());

        return allBuilders.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().typeName(),
                entry -> {
                    Pair<RelationshipsBuilder, RelationshipsBuilder> builders = entry.getValue();
                    RelationshipsBuilder outgoingRelationshipsBuilder = builders.getOne();
                    RelationshipsBuilder incomingRelationshipsBuilder = builders.getTwo();

                    AdjacencyList outAdjacencyList = outgoingRelationshipsBuilder != null
                            ? outgoingRelationshipsBuilder.adjacency.build() : null;
                    AdjacencyOffsets outAdjacencyOffsets = outgoingRelationshipsBuilder != null
                            ? outgoingRelationshipsBuilder.globalAdjacencyOffsets : null;

                    AdjacencyList inAdjacencyList = incomingRelationshipsBuilder != null
                            ? incomingRelationshipsBuilder.adjacency.build() : null;
                    AdjacencyOffsets inAdjacencyOffsets = incomingRelationshipsBuilder != null
                            ? incomingRelationshipsBuilder.globalAdjacencyOffsets : null;

                    long relationshipCount = relationshipCounts.getOrDefault(entry.getKey(), 0L);

                    // In non-legacy mode, the factory loads at most one adjacency list per projection.
                    // We store that adjacency list always in the outgoing adjacency list.
                    // Algorithms that run in non-legacy mode default to Direction.OUTGOING when accessing
                    // the graph internals (via relationship iterations or degree access).
                    if (!setup.legacyMode()) {
                        if (outgoingRelationshipsBuilder != null && incomingRelationshipsBuilder != null) {
                            throw new IllegalStateException("GraphSetup is set to non-legacy mode, but loads two adjacency lists.");
                        }

                        if (inAdjacencyList != null) {
                            outAdjacencyList = inAdjacencyList;
                            inAdjacencyList = null;
                            outAdjacencyOffsets = inAdjacencyOffsets;
                            inAdjacencyOffsets = null;
                            outgoingRelationshipsBuilder = incomingRelationshipsBuilder;
                            incomingRelationshipsBuilder = null;
                        }
                    }

                    if (!dimensions.relationshipProperties().hasMappings()) {
                        HugeGraph graph = HugeGraph.create(
                                tracker,
                                idsAndProperties.hugeIdMap,
                                idsAndProperties.properties,
                                outAdjacencyList,
                                outAdjacencyOffsets,
                                inAdjacencyList,
                                inAdjacencyOffsets,
                                relationshipCount,
                            setup.loadAsUndirected()
                        );
                        return Collections.singletonMap(ANY_REL_TYPE, graph);
                    } else {
                        AdjacencyList finalOutAdjacencyList = outAdjacencyList;
                        AdjacencyOffsets finalOutAdjacencyOffsets = outAdjacencyOffsets;
                        AdjacencyList finalInAdjacencyList = inAdjacencyList;
                        AdjacencyOffsets finalInAdjacencyOffsets = inAdjacencyOffsets;
                        RelationshipsBuilder finalOutgoingRelationshipsBuilder = outgoingRelationshipsBuilder;
                        RelationshipsBuilder finalIncomingRelationshipsBuilder = incomingRelationshipsBuilder;

                        return dimensions.relationshipProperties().enumerate().map(propertyEntry -> {
                            int weightIndex = propertyEntry.getOne();
                            ResolvedPropertyMapping property = propertyEntry.getTwo();
                            HugeGraph graph = create(
                                    tracker,
                                    idsAndProperties.hugeIdMap,
                                    idsAndProperties.properties,
                                    finalIncomingRelationshipsBuilder,
                                    finalOutgoingRelationshipsBuilder,
                                    finalOutAdjacencyList,
                                    finalOutAdjacencyOffsets,
                                    finalInAdjacencyList,
                                    finalInAdjacencyOffsets,
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

    private Pair<RelationshipsBuilder, RelationshipsBuilder> createBuildersForRelationshipType(AllocationTracker tracker) {
        RelationshipsBuilder outgoingRelationshipsBuilder = null;
        RelationshipsBuilder incomingRelationshipsBuilder = null;

        DeduplicationStrategy[] deduplicationStrategies = dimensions
                .relationshipProperties()
                .stream()
                .map(property -> property.deduplicationStrategy() == DeduplicationStrategy.DEFAULT
                        ? DeduplicationStrategy.SINGLE
                        : property.deduplicationStrategy()
                )
                .toArray(DeduplicationStrategy[]::new);
        // TODO: backwards compat code
        if (deduplicationStrategies.length == 0) {
            DeduplicationStrategy deduplicationStrategy =
                setup.deduplicationStrategy() == DeduplicationStrategy.DEFAULT
                            ? DeduplicationStrategy.SINGLE
                            : setup.deduplicationStrategy();
            deduplicationStrategies = new DeduplicationStrategy[]{deduplicationStrategy};
        }

        if (setup.loadAsUndirected()) {
            outgoingRelationshipsBuilder = new RelationshipsBuilder(
                    deduplicationStrategies,
                    tracker,
                    setup.relationshipPropertyMappings().numberOfMappings());
        } else {
            if (setup.loadOutgoing()) {
                outgoingRelationshipsBuilder = new RelationshipsBuilder(
                        deduplicationStrategies,
                        tracker,
                        setup.relationshipPropertyMappings().numberOfMappings());
            }
            if (setup.loadIncoming()) {
                incomingRelationshipsBuilder = new RelationshipsBuilder(
                        deduplicationStrategies,
                        tracker,
                        setup.relationshipPropertyMappings().numberOfMappings());
            }
        }

        return Tuples.pair(outgoingRelationshipsBuilder, incomingRelationshipsBuilder);
    }

    private HugeGraph create(
        AllocationTracker tracker,
        IdMap idMapping,
        Map<String, NodeProperties> nodeProperties,
        RelationshipsBuilder inRelationshipsBuilder,
        RelationshipsBuilder outRelationshipsBuilder,
        AdjacencyList outAdjacencyList,
        AdjacencyOffsets outAdjacencyOffsets,
        AdjacencyList inAdjacencyList,
        AdjacencyOffsets inAdjacencyOffsets,
        int weightIndex,
        ResolvedPropertyMapping weightProperty,
        long relationshipCount,
        boolean loadAsUndirected) {

        AdjacencyList outWeightList = null;
        AdjacencyOffsets outWeightOffsets = null;
        if (outRelationshipsBuilder != null) {
            if (weightProperty.propertyKeyId() != StatementConstants.NO_SUCH_PROPERTY_KEY) {
                outWeightOffsets = outRelationshipsBuilder.globalWeightOffsets[weightIndex];
                if (outWeightOffsets != null) {
                    outWeightList = outRelationshipsBuilder.weights[weightIndex].build();
                }
            }
        }

        AdjacencyList inWeightList = null;
        AdjacencyOffsets inWeightOffsets = null;
        if (inRelationshipsBuilder != null) {
            if (weightProperty.exists()) {
                inWeightOffsets = inRelationshipsBuilder.globalWeightOffsets[weightIndex];
                if (inWeightOffsets != null) {
                    inWeightList = inRelationshipsBuilder.weights[weightIndex].build();
                }
            }
        }

        Optional<Double> maybeDefaultWeight = weightProperty.exists()
                ? Optional.empty()
                : Optional.of(weightProperty.defaultValue());

        return HugeGraph.create(
            tracker,
            idMapping,
            nodeProperties,
            relationshipCount,
            inAdjacencyList,
            outAdjacencyList,
            inAdjacencyOffsets,
            outAdjacencyOffsets,
            maybeDefaultWeight,
            Optional.ofNullable(inWeightList),
            Optional.ofNullable(outWeightList),
            Optional.ofNullable(inWeightOffsets),
            Optional.ofNullable(outWeightOffsets),
            loadAsUndirected);
    }

}
