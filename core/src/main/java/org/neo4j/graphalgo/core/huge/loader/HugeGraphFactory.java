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

import org.neo4j.graphalgo.PropertyMapping;
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

public final class HugeGraphFactory extends GraphFactory {

    // TODO: make this configurable from somewhere
    private static final boolean LOAD_DEGREES = false;

    public HugeGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        MemoryEstimations.Builder builder = MemoryEstimations
                .builder(HugeGraph.class)
                .add("nodeIdMap", IdMap.memoryEstimation());

        // Node properties
        for (PropertyMapping propertyMapping : setup.nodePropertyMappings) {
            String propertyKey = propertyMapping.propertyName;
            int propertyId = dimensions.nodePropertyKeyId(propertyKey, setup);
            if (propertyId == StatementConstants.NO_SUCH_PROPERTY_KEY) {
                builder.add(propertyKey, HugeNullWeightMap.MEMORY_USAGE);
            } else {
                builder.add(propertyKey, HugeNodePropertyMap.memoryEstimation());
            }
        }

        // Relationship weight properties
        if (dimensions.relWeightId() == StatementConstants.NO_SUCH_PROPERTY_KEY) {
            builder.add(setup.relationWeightPropertyName, HugeNullWeightMap.MEMORY_USAGE);
        } else {
            builder.add(
                    setup.relationWeightPropertyName,
                    HugeWeightMap.memoryEstimation(setup.relationWeightPropertyName));
        }

        // Adjacency lists and Adjacency offsets
        MemoryEstimation adjacencyListSize = HugeAdjacencyList.memoryEstimation(setup.loadAsUndirected);
        MemoryEstimation adjacencyOffsetsSetup = HugeAdjacencyOffsets.memoryEstimation();
        if (this.setup.loadOutgoing || this.setup.loadAsUndirected) {
            builder.add("outgoing", adjacencyListSize);
            builder.add("outgoing offsets", adjacencyOffsetsSetup);

        }
        if (this.setup.loadIncoming && !this.setup.loadAsUndirected) {
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
        HugeAdjacencyBuilder outAdjacency = null;
        HugeAdjacencyBuilder inAdjacency = null;
        if (setup.loadAsUndirected) {
            outAdjacency = new HugeAdjacencyBuilder(tracker);
        } else {
            if (setup.loadOutgoing) {
                outAdjacency = new HugeAdjacencyBuilder(tracker);
            }
            if (setup.loadIncoming) {
                inAdjacency = new HugeAdjacencyBuilder(tracker);
            }
        }

        int weightProperty = dimensions.relWeightId();
        HugeWeightMapBuilder weightsBuilder = weightProperty == StatementConstants.NO_SUCH_PROPERTY_KEY
                ? new HugeWeightMapBuilder.NullBuilder(setup.relationDefaultWeight)
                : new HugeWeightMapBuilder(tracker, weightProperty, setup.relationDefaultWeight);

        long relationshipCount = new ScanningRelationshipsImporter(
                setup, api, dimensions, progress, tracker, idsAndProperties.hugeIdMap, weightsBuilder,
                LOAD_DEGREES, outAdjacency, inAdjacency, threadPool, concurrency)
                .call(setup.log);

        HugeWeightMapping weights = weightsBuilder.build();
        return HugeAdjacencyBuilder.apply(
                tracker,
                idsAndProperties.hugeIdMap,
                weights,
                idsAndProperties.properties,
                inAdjacency,
                outAdjacency,
                relationshipCount);
    }

}
