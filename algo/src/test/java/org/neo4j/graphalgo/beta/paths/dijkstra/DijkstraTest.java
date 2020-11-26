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
package org.neo4j.graphalgo.beta.paths.dijkstra;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
final class DijkstraTest {

    static ImmutableDijkstraStreamConfig.Builder defaultConfigBuilder() {
        return ImmutableDijkstraStreamConfig.builder()
            .path(true)
            .concurrency(1);
    }

    @Nested
    class ShortestPathWithDirectedWeights {

        // https://en.wikipedia.org/wiki/Shortest_path_problem#/media/File:Shortest_path_with_direct_weights.svg
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label1)" +
            ", (b:Label1)" +
            ", (c:Label1)" +
            ", (d:Label1)" +
            ", (e:Label1)" +
            ", (f:Label1)" +

            ", (a)-[:TYPE1 {cost: 4}]->(b)" +
            ", (a)-[:TYPE1 {cost: 2}]->(c)" +
            ", (b)-[:TYPE1 {cost: 5}]->(c)" +
            ", (b)-[:TYPE1 {cost: 10}]->(d)" +
            ", (c)-[:TYPE1 {cost: 3}]->(e)" +
            ", (d)-[:TYPE1 {cost: 11}]->(f)" +
            ", (e)-[:TYPE1 {cost: 4}]->(d)";

        @Inject
        private Graph graph;

        @Inject
        private IdFunction idFunction;

        @Test
        void test() {
            var expected = expected(graph, idFunction, "a", "c", "e", "d", "f");
            var nodeIds = expected.nodeIds;

            var config = defaultConfigBuilder()
                .sourceNode(nodeIds[0])
                .targetNode(nodeIds[nodeIds.length - 1])
                .build();

            var dijkstraResult = new Dijkstra(graph, config, AllocationTracker.empty()).compute();
            var path = dijkstraResult.paths().get(0);
            assertEquals(expected.weight, path.totalCost, 0.1);

            var ids = path.nodeIds.stream().mapToLong(graph::toOriginalNodeId).toArray();
            assertArrayEquals(nodeIds, ids);
        }
    }

    @Nested
    class MoreComplexGraph {

        // https://www.cise.ufl.edu/~sahni/cop3530/slides/lec326.pdf
        // without the additional 14 edge
        @GdlGraph
        private static final String DB_CYPHER2 =
            "CREATE" +
            "  (n1:Label2)" +
            ", (n2:Label2)" +
            ", (n3:Label2)" +
            ", (n4:Label2)" +
            ", (n5:Label2)" +
            ", (n6:Label2)" +
            ", (n7:Label2)" +

            ", (n1)-[:TYPE2 {cost: 6}]->(n2)" +
            ", (n1)-[:TYPE2 {cost: 2}]->(n3)" +
            ", (n1)-[:TYPE2 {cost: 16}]->(n4)" +
            ", (n2)-[:TYPE2 {cost: 4}]->(n5)" +
            ", (n2)-[:TYPE2 {cost: 5}]->(n4)" +
            ", (n3)-[:TYPE2 {cost: 7}]->(n2)" +
            ", (n3)-[:TYPE2 {cost: 3}]->(n5)" +
            ", (n3)-[:TYPE2 {cost: 8}]->(n6)" +
            ", (n4)-[:TYPE2 {cost: 7}]->(n3)" +
            ", (n5)-[:TYPE2 {cost: 4}]->(n4)" +
            ", (n5)-[:TYPE2 {cost: 10}]->(n7)" +
            ", (n6)-[:TYPE2 {cost: 1}]->(n7)";

        @Inject
        private Graph graph;

        @Inject
        private IdFunction idFunction;

        @Test
        void test() {
            var expected = expected(graph, idFunction, "n1", "n3", "n6", "n7");
            var nodeIds = expected.nodeIds;

            var config = defaultConfigBuilder()
                .sourceNode(nodeIds[0])
                .targetNode(nodeIds[nodeIds.length - 1])
                .build();

            var dijkstraResult = new Dijkstra(graph, config, AllocationTracker.empty()).compute();
            var path = dijkstraResult.paths().get(0);
            assertEquals(expected.weight, path.totalCost, 0.1);

            var ids = path.nodeIds.stream().mapToLong(graph::toOriginalNodeId).toArray();
            assertArrayEquals(nodeIds, ids);
        }
    }

    private ShortestPath expected(RelationshipProperties graph, IdFunction idFunction, String... nodeIds) {

        var nodes = new long[nodeIds.length];
        var weight = 0.0;
        for (int i = 0; i < nodeIds.length; i++) {
            var currentNode = idFunction.of(nodeIds[i]);
            var j = i + 1;
            if (j < nodeIds.length) {
                var nextNode = idFunction.of(nodeIds[j]);
                weight += graph.relationshipProperty(currentNode, nextNode);
            }
            nodes[i] = currentNode;
        }

        return new ShortestPath(nodes, weight);
    }

    private static final class ShortestPath {
        private final long[] nodeIds;
        private final double weight;

        private ShortestPath(long[] nodeIds, double weight) {
            this.nodeIds = nodeIds;
            this.weight = weight;
        }
    }
}
