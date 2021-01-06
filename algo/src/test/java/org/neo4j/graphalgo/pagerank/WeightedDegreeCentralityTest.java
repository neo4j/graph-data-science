/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@GdlExtension
final class WeightedDegreeCentralityTest {

    @GdlGraph(graphNamePrefix = "natural", orientation = Orientation.NATURAL)
    @GdlGraph(graphNamePrefix = "reverse", orientation = Orientation.REVERSE)
    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label)" +
            ", (b:Label)" +
            ", (c:Label)" +
            ", (d:Label)" +
            ", (e:Label)" +
            ", (f:Label)" +
            ", (g:Label)" +
            ", (h:Label)" +
            ", (i:Label)" +
            ", (j:Label)" +

            ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (d)-[:TYPE1 {weight: 5.0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 7.0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 1.0}]->(f)" +
            ", (f)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (f)-[:TYPE1 {weight: 2.0}]->(e)" +

            ", (g)-[:TYPE2]->(b)" +
            ", (g)-[:TYPE2]->(e)" +
            ", (h)-[:TYPE2]->(b)" +
            ", (h)-[:TYPE2]->(e)" +
            ", (i)-[:TYPE2]->(b)" +
            ", (i)-[:TYPE2]->(e)" +
            ", (j)-[:TYPE2]->(e)" +
            ", (k)-[:TYPE2]->(e)" +

            ", (a)-[:TYPE3 {weight: -2.0}]->(b)" +
            ", (b)-[:TYPE3 {weight: 2.0}]->(c)" +
            ", (c)-[:TYPE3 {weight: 2.0}]->(b)" +
            ", (d)-[:TYPE3 {weight: 2.0}]->(a)" +
            ", (d)-[:TYPE3 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE3 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE3 {weight: 2.0}]->(d)" +
            ", (e)-[:TYPE3 {weight: 2.0}]->(f)" +
            ", (f)-[:TYPE3 {weight: 2.0}]->(b)" +
            ", (f)-[:TYPE3 {weight: 2.0}]->(e)";

    @Inject
    private GraphStore naturalGraphStore;

    @Inject
    private IdFunction naturalIdFunction;

    @Inject
    private GraphStore reverseGraphStore;

    @Test
    void buildWeightsArray() {
        var expected = Map.of(
            naturalIdFunction.of("a"), new double[]{},
            naturalIdFunction.of("b"), new double[]{2.0},
            naturalIdFunction.of("c"), new double[]{2.0},
            naturalIdFunction.of("d"), new double[]{5.0, 2.0},
            naturalIdFunction.of("e"), new double[]{2.0, 7.0, 1.0},
            naturalIdFunction.of("f"), new double[]{2.0, 2.0},
            naturalIdFunction.of("g"), new double[]{},
            naturalIdFunction.of("h"), new double[]{},
            naturalIdFunction.of("i"), new double[]{},
            naturalIdFunction.of("j"), new double[]{}
        );

        var graph = naturalGraphStore.getGraph(
            List.of(NodeLabel.of("Label")),
            List.of(RelationshipType.of("TYPE1")),
            Optional.of("weight")
        );

        var result = new WeightedDegreeCentrality(
            graph,
            1,
            true,
            Pools.DEFAULT,
            AllocationTracker.empty()
        ).compute().weights();

        expected.forEach((originalNodeId, expectedPageRank) -> {
            assertArrayEquals(
                expected.get(originalNodeId),
                result.get(graph.toMappedNodeId(originalNodeId)).toArray(),
                1e-2,
                "Node#" + originalNodeId
            );
        });
    }

    @Test
    void shouldThrowIfGraphHasNoRelationshipProperty() {
        var graph = naturalGraphStore.getGraph(
            List.of(NodeLabel.of("Label")),
            List.of(RelationshipType.of("TYPE1")),
            Optional.empty()
        );

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class, () -> {
            new WeightedDegreeCentrality(
                graph,
                1,
                false,
                Pools.DEFAULT,
                AllocationTracker.empty()
            );
        });

        assertEquals(
            "WeightedDegreeCentrality requires a weight property to be loaded.",
            exception.getMessage()
        );
    }

    @Test
    void weightedOutgoingCentrality() {
        var expected = Map.of(
            naturalIdFunction.of("a"), 0.0,
            naturalIdFunction.of("b"), 2.0,
            naturalIdFunction.of("c"), 2.0,
            naturalIdFunction.of("d"), 7.0,
            naturalIdFunction.of("e"), 10.0,
            naturalIdFunction.of("f"), 4.0,
            naturalIdFunction.of("g"), 0.0,
            naturalIdFunction.of("h"), 0.0,
            naturalIdFunction.of("i"), 0.0,
            naturalIdFunction.of("j"), 0.0
        );

        var graph = naturalGraphStore.getGraph(
            List.of(NodeLabel.of("Label")),
            List.of(RelationshipType.of("TYPE1")),
            Optional.of("weight")
        );

        assertDegrees(graph, expected, 1);
    }

    @Test
    void excludeNegativeWeights() {
        var expected = Map.of(
            naturalIdFunction.of("a"), 0.0,
            naturalIdFunction.of("b"), 2.0,
            naturalIdFunction.of("c"), 2.0,
            naturalIdFunction.of("d"), 4.0,
            naturalIdFunction.of("e"), 6.0,
            naturalIdFunction.of("f"), 4.0,
            naturalIdFunction.of("g"), 0.0,
            naturalIdFunction.of("h"), 0.0,
            naturalIdFunction.of("i"), 0.0,
            naturalIdFunction.of("j"), 0.0
        );

        var graph = naturalGraphStore.getGraph(
            List.of(NodeLabel.of("Label")),
            List.of(RelationshipType.of("TYPE3")),
            Optional.of("weight")
        );

        assertDegrees(graph, expected, 1);
    }

    @Test
    void weightedIncomingCentrality() {
        var expected = Map.of(
            naturalIdFunction.of("a"), 5.0,
            naturalIdFunction.of("b"), 8.0,
            naturalIdFunction.of("c"), 2.0,
            naturalIdFunction.of("d"), 7.0,
            naturalIdFunction.of("e"), 2.0,
            naturalIdFunction.of("f"), 1.0,
            naturalIdFunction.of("g"), 0.0,
            naturalIdFunction.of("h"), 0.0,
            naturalIdFunction.of("i"), 0.0,
            naturalIdFunction.of("j"), 0.0
        );

        var graph = reverseGraphStore.getGraph(
            List.of(NodeLabel.of("Label")),
            List.of(RelationshipType.of("TYPE1")),
            Optional.of("weight")
        );

        assertDegrees(graph, expected, 4);
    }

    private void assertDegrees(Graph graph, Map<Long, Double> expected, int concurrency) {
        var result = new WeightedDegreeCentrality(
            graph,
            concurrency,
            false,
            Pools.DEFAULT,
            AllocationTracker.empty()
        ).compute().degrees();

        expected.forEach((originalNodeId, expectedPageRank) -> {
            assertEquals(
                expected.get(originalNodeId),
                result.get(graph.toMappedNodeId(originalNodeId)),
                1e-2,
                "Node#" + originalNodeId
            );
        });
    }
}
