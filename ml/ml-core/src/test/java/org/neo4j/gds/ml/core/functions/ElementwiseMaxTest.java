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
package org.neo4j.gds.ml.core.functions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.NeighborhoodFunction;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.subgraph.SubGraph;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class ElementwiseMaxTest extends ComputationGraphBaseTest implements FiniteDifferenceTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (u1:User { id: 0 })" +
        ", (u2:User { id: 1 })" +
        ", (d1:Dish { id: 2 })" +
        ", (d2:Dish { id: 3 })" +
        ", (d3:Dish { id: 4 })" +
        ", (d4:Dish { id: 5 })" +
        ", (u1)-[:ORDERED {times: 5}]->(d1)" +
        ", (u1)-[:ORDERED {times: 2}]->(d2)" +
        ", (u1)-[:ORDERED {times: 1}]->(d3)" +
        ", (u2)-[:ORDERED {times: 2}]->(d3)" +
        ", (u2)-[:ORDERED {times: 3}]->(d4)";

    @Inject
    TestGraph graph;

    @Test
    void testApplyUnweighted() {
        // (b)->(a), (b)->(b), (b)->(c)
        var adjacencyMatrix = new int[2][3];
        adjacencyMatrix[0] = new int[]{};
        adjacencyMatrix[1] = new int[]{0, 1, 2};

        int[] batchIds = {1, 0};
        var unweightedGraph = TestBatchNeighbors.of(batchIds, adjacencyMatrix);

        var parent = Constant.matrix(new double[]{
            1, 2, 3,
            5, 2, 1,
            9, 4, 2,
            1, 1, 1
        }, 4, 3);

        Variable<Matrix> max = new ElementWiseMax(parent, unweightedGraph);

        var expected = new Matrix(new double[]{
            9, 4, 3,    // Node 1
            0, 0, 0    // Node 0 --> no neighbours --> 0s
        }, 2, 3);

        assertThat(ctx.forward(max)).isEqualTo(expected);
    }

    @Test
    void shouldApproximateGradientUnweighted() {
        var weights = new Weights<>(new Matrix(new double[]{
            1, 2, 3,
            3, 2, 1,
            1, 3, 2
        }, 3, 3));

        // (b)->(a), (b)->(b), (b)->(c)
        int[][] adjacencyMatrix = {
            new int[]{},
            new int[]{0, 1, 2},
            new int[]{}
        };
        int[] batchIds = {1, 2, 0};
        var unweightedGraph = TestBatchNeighbors.of(batchIds, adjacencyMatrix);

        var loss = new ElementSum(List.of(new ElementWiseMax(
            weights,
            unweightedGraph
        )));

        finiteDifferenceShouldApproximateGradient(weights, loss);
    }

    @Test
    void shouldApproximateGradientWithSmallDiffBetweenNeighbors() {
        // (a)->(a), (b)->(a), (b)->(b)
        int[][] adjacencyMatrix = {
            new int[]{0},
            new int[]{0, 1}
        };
        var unweightedGraph = TestBatchNeighbors.of(adjacencyMatrix);

        var weights = new Weights<>(new Matrix(new double[]{
            1.0000009,
            1.0000004
        }, 2, 1));

        var loss = new ElementSum(List.of(new ElementWiseMax(weights, unweightedGraph)));

        ComputationContext ctx = new ComputationContext();

        ctx.forward(loss);
        ctx.backward(loss);

        var expected = new Matrix(
            new double[]{2.0, 0.0},
            2, 1
        );

        assertThat(ctx.gradient(weights)).isEqualTo(expected);
    }

    @Test
    void testUnweightedGradientWithUnorderedBatchIds() {
        // (a)->(c), (b)->(b), (b)->(b)
        int[][] adj = new int[2][];
        adj[0] = new int[]{2};
        adj[1] = new int[]{0, 1};
        int[] batchIds = {1};
        TestBatchNeighbors unweightedGraph = TestBatchNeighbors.of(batchIds, adj);

        Weights<Matrix> weights = new Weights<>(new Matrix(new double[]{1, 4, 3}, 3, 1));

        var loss = new ElementSum(List.of(new ElementWiseMax(weights, unweightedGraph)));

        ComputationContext ctx = new ComputationContext();

        ctx.forward(loss);
        ctx.backward(loss);

        var expected = new Matrix(
            new double[]{0.0, 1.0, 0.0},
            3, 1
        );

        assertThat(ctx.gradient(weights)).isEqualTo(expected);
    }

    @Test
    void shouldApplyWeightsToEmbeddings() {
        long[] ids = new long[]{
            graph.toMappedNodeId("d1"),
            graph.toMappedNodeId("d2"),
            graph.toMappedNodeId("d3"),
            graph.toMappedNodeId("d4"),
        };
        NeighborhoodFunction neighborhoodFunction = (nodeId) -> graph
            .streamRelationships(nodeId, 0.0D)
            .mapToLong(RelationshipCursor::targetId);

        var subGraph = SubGraph.buildSubGraph(ids, neighborhoodFunction, graph::relationshipProperty);

        var userEmbeddings = Constant.matrix(new double[]{
            1, 1, 1, // u1
            1, 1, 1, // u2
            1, 1, 1, // d1
            3, 3, 3, // d2
            1, 1, 1, // d3
            1, 1, 1 // d4
        }, 6, 3);

        var weightedEmbeddings = new ElementWiseMax(
            userEmbeddings,
            subGraph
        );

        var expected = new Matrix(new double[]{
            5.0, 5.0, 5.0, // d1
            2.0, 2.0, 2.0, // d2
            2.0, 2.0, 2.0, // d3
            3.0, 3.0, 3.0, // d4
        }, 4, 3);

        // Add userEmbeddings to context's data
        ctx.forward(userEmbeddings);
        assertThat(weightedEmbeddings.apply(ctx)).isEqualTo(expected);
    }

    @Test
    void testWeightedGradient() {
        long[] ids = new long[]{
            graph.toMappedNodeId("d1"),
            graph.toMappedNodeId("d2"),
            graph.toMappedNodeId("d3"),
            graph.toMappedNodeId("d4"),
        };
        NeighborhoodFunction neighborhoodFunction = (nodeId) -> graph
            .streamRelationships(nodeId, 0.0D)
            .mapToLong(RelationshipCursor::targetId);
        var subGraph = SubGraph.buildSubGraph(ids, neighborhoodFunction, graph::relationshipProperty);

        var weights = new Weights<>(new Matrix(new double[]{
            1, 1, 1, // u1
            2, 2, 2, // u2
            3, 3, 3, // d1
            4, 4, 4, // d2
            5, 5, 5, // d3
            6, 6, 6 // d4
        }, 6, 3));

        finiteDifferenceShouldApproximateGradient(
            weights,
            new ElementSum(List.of(new ElementWiseMax(weights, subGraph)))
        );
    }
}
