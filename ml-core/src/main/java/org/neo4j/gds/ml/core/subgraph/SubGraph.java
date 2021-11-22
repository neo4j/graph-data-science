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
import java.util.Optional;

public class SubGraph implements BatchNeighbors {
    // mapped node ids in the original input batch
    public final int[] mappedBatchNodeIds;

    // this includes all nodes part of the subgraph
    // long-based ids used in org.neo4j.gds.api.Graph
    public final long[] originalNodeIds;

    // stores the sampled neighbors for each node in the input batch
    final int[][] neighbors;

    private final Optional<RelationshipWeights> maybeRelationshipWeightsFunction;

    public SubGraph(
        int[][] neighborsPerBatchedNode,
        int[] nodeIds,
        long[] originalNodeIds,
        Optional<RelationshipWeights> maybeRelationshipWeightsFunction
    ) {
        this.neighbors = neighborsPerBatchedNode;
        this.mappedBatchNodeIds = nodeIds;
        this.originalNodeIds = originalNodeIds;
        this.maybeRelationshipWeightsFunction = maybeRelationshipWeightsFunction;
    }

    public static List<SubGraph> buildSubGraphs(
        long[] nodeIds,
        List<NeighborhoodFunction> neighborhoodFunctions,
        Graph graph,
        boolean useWeights
    ) {
        List<SubGraph> result = new ArrayList<>();
        long[] previousNodes = nodeIds;

        for (NeighborhoodFunction neighborhoodFunction : neighborhoodFunctions) {
            SubGraph lastGraph = buildSubGraph(previousNodes, neighborhoodFunction, graph, useWeights);
            result.add(lastGraph);
            previousNodes = lastGraph.originalNodeIds;
        }
        return result;
    }

    public static SubGraph buildSubGraph(long[] nodeIds, NeighborhoodFunction neighborhoodFunction, Graph graph) {
        return buildSubGraph(nodeIds, neighborhoodFunction, graph, false);
    }

    public static SubGraph buildSubGraph(long[] batchNodeIds, NeighborhoodFunction neighborhoodFunction, Graph graph, boolean useWeights) {
        int[][] adjacency = new int[batchNodeIds.length][];
        int[] batchedNodeIds = new int[batchNodeIds.length];

        // mapping original long-based nodeIds into consecutive int-based ids
        LocalIdMap idmap = new LocalIdMap();

        // map the input node ids
        // this assures they are in consecutive order
        for (long nodeId : batchNodeIds) {
            idmap.toMapped(nodeId);
        }

        for (int nodeOffset = 0, nodeIdsLength = batchNodeIds.length; nodeOffset < nodeIdsLength; nodeOffset++) {
            long nodeId = batchNodeIds[nodeOffset];

            batchedNodeIds[nodeOffset] = idmap.toMapped(nodeId);

            var nodeNeighbors = neighborhoodFunction.apply(graph, nodeId);

            // map sampled neighbors into local id space
            // this also expands the id mapping as the neighbours could be not in the nodeIds[]
            int[] neighborInternalIds = nodeNeighbors
                .mapToInt(idmap::toMapped)
                .toArray();

            adjacency[nodeOffset] = neighborInternalIds;
        }

        return new SubGraph(adjacency, batchedNodeIds, idmap.originalIds(), relationshipWeightFunction(graph, useWeights));
    }

    public int batchSize() {
        return mappedBatchNodeIds.length;
    }

    public int[] neighbors(int nodeId) {
        return neighbors[nodeId];
    }

    public boolean isWeighted() {
        return maybeRelationshipWeightsFunction.isPresent();
    }

    public double relWeight(int src, int trg) {
        return maybeRelationshipWeightsFunction.orElseThrow().weight(originalNodeIds[src], originalNodeIds[trg]);
    }

    private static Optional<RelationshipWeights> relationshipWeightFunction(Graph graph, boolean useWeights) {
        return useWeights
            ? Optional.of(graph::relationshipProperty)
            : Optional.empty();
    }

}
