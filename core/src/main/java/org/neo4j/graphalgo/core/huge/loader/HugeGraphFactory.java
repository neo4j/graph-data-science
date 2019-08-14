/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.KernelPropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.huge.HugeAdjacencyList;
import org.neo4j.graphalgo.core.huge.HugeAdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.ApproximatedImportProgress;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;

public final class HugeGraphFactory extends GraphFactory {

    // TODO: make this configurable from somewhere
    private static final boolean LOAD_DEGREES = false;

    public HugeGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return getMemoryEstimation(setup, dimensions);
    }

    public static MemoryEstimation getMemoryEstimation(
            final GraphSetup setup,
            final GraphDimensions dimensions) {
        MemoryEstimations.Builder builder = MemoryEstimations
                .builder(HugeGraph.class)
                .add("nodeIdMap", IdMap.memoryEstimation());

        // Node properties
        for (KernelPropertyMapping propertyMapping : dimensions.nodeProperties()) {
            int propertyId = propertyMapping.propertyKeyId;
            if (propertyId == StatementConstants.NO_SUCH_PROPERTY_KEY) {
                builder.add(propertyMapping.propertyName, HugeNullWeightMap.MEMORY_USAGE);
            } else {
                builder.add(propertyMapping.propertyName, HugeNodePropertyMap.memoryEstimation());
            }
        }

        // Relationship weight properties
        if (dimensions.relWeightId() != StatementConstants.NO_SUCH_PROPERTY_KEY) {
            // Adjacency lists and Adjacency offsets
            MemoryEstimation adjacencyListSize = HugeAdjacencyList.uncompressedMemoryEstimation(setup.loadAsUndirected);
            MemoryEstimation adjacencyOffsetsSetup = HugeAdjacencyOffsets.memoryEstimation();
            if (setup.loadOutgoing || setup.loadAsUndirected) {
                builder.add("outgoing weights", adjacencyListSize);
                builder.add("outgoing weight offsets", adjacencyOffsetsSetup);

            }
            if (setup.loadIncoming && !setup.loadAsUndirected) {
                builder.add("incoming weights", adjacencyListSize);
                builder.add("incoming weight offsets", adjacencyOffsetsSetup);
            }
        }

        // Adjacency lists and Adjacency offsets
        MemoryEstimation adjacencyListSize = HugeAdjacencyList.compressedMemoryEstimation(setup.loadAsUndirected);
        MemoryEstimation adjacencyOffsetsSetup = HugeAdjacencyOffsets.memoryEstimation();
        if (setup.loadOutgoing || setup.loadAsUndirected) {
            builder.add("outgoing", adjacencyListSize);
            builder.add("outgoing offsets", adjacencyOffsetsSetup);

        }
        if (setup.loadIncoming && !setup.loadAsUndirected) {
            builder.add("incoming", adjacencyListSize);
            builder.add("incoming offsets", adjacencyOffsetsSetup);
        }

        return builder.build();
    }

    @Override
    protected ImportProgress importProgress(
            final ProgressLogger progressLogger,
            final GraphDimensions dimensions,
            final GraphSetup setup) {

        // ops for scanning degrees
        long relOperations = LOAD_DEGREES ? dimensions.maxRelCount() : 0L;

        // batching for undirected double the amount of rels imported
        if (setup.loadIncoming || setup.loadAsUndirected) {
            relOperations += dimensions.maxRelCount();
        }
        if (setup.loadOutgoing || setup.loadAsUndirected) {
            relOperations += dimensions.maxRelCount();
        }

        return new ApproximatedImportProgress(
                progressLogger,
                setup.tracker,
                dimensions.nodeCount(),
                relOperations
        );
    }

    @Override
    public Graph importGraph() {
        GraphDimensions dimensions = this.dimensions;
        int concurrency = setup.concurrency();
        AllocationTracker tracker = setup.tracker;
        IdsAndProperties mappingAndProperties = loadHugeIdMap(tracker, concurrency);
        Graph graph = loadRelationships(dimensions, tracker, mappingAndProperties, concurrency);
        progressLogger.logDone(tracker);
        return graph;
    }

    private IdsAndProperties loadHugeIdMap(AllocationTracker tracker, int concurrency) {
        return new ScanningNodesImporter(
                api,
                dimensions,
                progress,
                tracker,
                setup.terminationFlag,
                threadPool,
                concurrency,
                setup.nodePropertyMappings)
                .call(setup.log);
    }

    private Graph loadRelationships(
            GraphDimensions dimensions,
            AllocationTracker tracker,
            IdsAndProperties idsAndProperties,
            int concurrency) {
        RelationshipsBuilder outgoingRelationshipsBuilder = null;
        RelationshipsBuilder incomingRelationshipsBuilder = null;

        if (setup.loadAsUndirected) {
            outgoingRelationshipsBuilder = new RelationshipsBuilder(setup.duplicateRelationshipsStrategy, tracker, setup.shouldLoadRelationshipWeight());
        } else {
            if (setup.loadOutgoing) {
                outgoingRelationshipsBuilder = new RelationshipsBuilder(setup.duplicateRelationshipsStrategy, tracker, setup.shouldLoadRelationshipWeight());
            }
            if (setup.loadIncoming) {
                incomingRelationshipsBuilder = new RelationshipsBuilder(setup.duplicateRelationshipsStrategy, tracker, setup.shouldLoadRelationshipWeight());
            }
        }

        long relationshipCount = new ScanningRelationshipsImporter(
                setup,
                api,
                dimensions,
                progress,
                tracker,
                idsAndProperties.hugeIdMap,
                outgoingRelationshipsBuilder,
                incomingRelationshipsBuilder,
                threadPool,
                concurrency)
                .call(setup.log);


        return buildGraph(
                tracker,
                idsAndProperties.hugeIdMap,
                idsAndProperties.properties,
                incomingRelationshipsBuilder,
                outgoingRelationshipsBuilder,
                setup.relationDefaultWeight,
                relationshipCount);
    }

    private Graph buildGraph(
            final AllocationTracker tracker,
            final IdMap idMapping,
            final Map<String, HugeWeightMapping> nodeProperties,
            final RelationshipsBuilder inRelationshipsBuilder,
            final RelationshipsBuilder outRelationshipsBuilder,
            final double defaultWeight,
            final long relationshipCount) {

        HugeAdjacencyList outAdjacencyList = null;
        HugeAdjacencyOffsets outAdjacencyOffsets = null;
        HugeAdjacencyList outWeightList = null;
        HugeAdjacencyOffsets outWeightOffsets = null;
        if (outRelationshipsBuilder != null) {
            outAdjacencyList = outRelationshipsBuilder.adjacency.build();
            outAdjacencyOffsets = outRelationshipsBuilder.globalAdjacencyOffsets;

            if (setup.shouldLoadRelationshipWeight()) {
                outWeightList = outRelationshipsBuilder.weights.build();
                outWeightOffsets = outRelationshipsBuilder.globalWeightOffsets;
            }
        }

        HugeAdjacencyList inAdjacencyList = null;
        HugeAdjacencyOffsets inAdjacencyOffsets = null;
        HugeAdjacencyList inWeightList = null;
        HugeAdjacencyOffsets inWeightOffsets = null;
        if (inRelationshipsBuilder != null) {
            inAdjacencyList = inRelationshipsBuilder.adjacency.build();
            inAdjacencyOffsets = inRelationshipsBuilder.globalAdjacencyOffsets;

            if (setup.shouldLoadRelationshipWeight()) {
                inWeightList = inRelationshipsBuilder.weights.build();
                inWeightOffsets = inRelationshipsBuilder.globalWeightOffsets;
            }
        }

        return new HugeGraph(
                tracker, idMapping, nodeProperties, relationshipCount,
                inAdjacencyList, outAdjacencyList, inAdjacencyOffsets, outAdjacencyOffsets,
                defaultWeight,
                inWeightList, outWeightList, inWeightOffsets, outWeightOffsets);
    }

}
