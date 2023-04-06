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
package org.neo4j.gds.ml.core.subgraph;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.ml.core.NeighborhoodFunction;
import org.neo4j.gds.ml.core.RelationshipWeights;

import java.util.ArrayList;
import java.util.List;

public final class SubGraph implements BatchNeighbors {
    // mapped node ids in the original input batch
    private final int[] mappedBatchNodeIds;

    // this includes all nodes part of the subgraph
    // long-based ids used in org.neo4j.gds.api.Graph
    private final long[] originalNodeIds;

    // stores the sampled neighbors for each node in the input batch
    final int[][] neighbors;

    private final RelationshipWeights relationshipWeightsFunction;

    private SubGraph(
        int[][] neighborsPerBatchedNode,
        int[] nodeIds,
        long[] originalNodeIds,
        RelationshipWeights relationshipWeightsFunction
    ) {
        this.neighbors = neighborsPerBatchedNode;
        this.mappedBatchNodeIds = nodeIds;
        this.originalNodeIds = originalNodeIds;
        this.relationshipWeightsFunction = relationshipWeightsFunction;
    }

    public static List<SubGraph> buildSubGraphs(
        long[] batchNodeIds,
        List<NeighborhoodFunction> neighborhoodFunctions,
        RelationshipWeights weightFunction
    ) {
        List<SubGraph> result = new ArrayList<>();
        long[] previousNodes = batchNodeIds;

        for (NeighborhoodFunction neighborhoodFunction : neighborhoodFunctions) {
            SubGraph lastGraph = buildSubGraph(previousNodes, neighborhoodFunction, weightFunction);
            result.add(lastGraph);
            // the next Subgraph needs to consider all nodes included in the last subgraph
            previousNodes = lastGraph.originalNodeIds;
        }
        return result;
    }

    public static SubGraph buildSubGraph(long[] batchNodeIds, NeighborhoodFunction neighborhoodFunction, RelationshipWeights weightFunction) {
        int[] mappedBatchNodeIds = new int[batchNodeIds.length];

        // mapping original long-based nodeIds into consecutive int-based ids
        LocalIdMap idmap = new LocalIdMap();

        // map the input node ids
        // this assures they are in consecutive order
        for (int nodeOffset = 0, nodeIdsLength = batchNodeIds.length; nodeOffset < nodeIdsLength; nodeOffset++) {
            int mappedNodeId = idmap.toMapped(batchNodeIds[nodeOffset]);
            mappedBatchNodeIds[nodeOffset] = mappedNodeId;
        }
        int[][] adjacency = new int[idmap.size()][];

        for (int mappedNodeId = 0, mappedBatchIds = idmap.size(); mappedNodeId < mappedBatchIds; mappedNodeId++) {

            var nodeNeighbors = neighborhoodFunction.sample(idmap.toOriginal(mappedNodeId));

            // map sampled neighbors into local id space
            // this also expands the id mapping as the neighbours could be not in the nodeIds[]
            int[] neighborInternalIds = nodeNeighbors
                .mapToInt(idmap::toMapped)
                .toArray();

            adjacency[mappedNodeId] = neighborInternalIds;
        }

        return new SubGraph(adjacency, mappedBatchNodeIds, idmap.originalIds(), weightFunction);
    }

    @Override
    public int[] batchIds() {
        return mappedBatchNodeIds;
    }

    public int nodeCount() {
        return originalNodeIds.length;
    }

    @Override
    public int degree(int batchId) {
        return neighbors[batchId].length;
    }

    public long[] originalNodeIds() {
        return originalNodeIds;
    }

    @Override
    public int[] neighbors(int nodeId) {
        return neighbors[nodeId];
    }

    public boolean isWeighted() {
        return relationshipWeightsFunction != RelationshipWeights.UNWEIGHTED;
    }

    @Override
    public double relationshipWeight(int src, int trg) {
        return relationshipWeightsFunction.weight(originalNodeIds[src], originalNodeIds[trg]);
    }

    public static RelationshipWeights relationshipWeightFunction(Graph graph) {
        return graph.hasRelationshipProperty() ? graph::relationshipProperty : RelationshipWeights.UNWEIGHTED;
    }

}
