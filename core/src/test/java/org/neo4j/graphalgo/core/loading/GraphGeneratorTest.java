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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class GraphGeneratorTest {

    private static final String EXPECTED_WITH_AGGREGATION =
        "(a)-[{w: 0.0}]->(b)-[{w: 2.0}]->(c)-[{w: 4.0}]->(d)-[{w: 6.0}]->(a)";

    private static final String EXPECTED_WITHOUT_AGGREGATION_GRAPH =
        "(a)-[{w: 0.0}]->(b)" +
        "(a)-[{w: 0.0}]->(b)" +
        "(b)-[{w: 1.0}]->(c)" +
        "(b)-[{w: 1.0}]->(c)" +
        "(c)-[{w: 2.0}]->(d)" +
        "(c)-[{w: 2.0}]->(d)" +
        "(d)-[{w: 3.0}]->(a)" +
        "(d)-[{w: 3.0}]->(a)";

    private static final String EXPECTED_UNWEIGHTED_GRAPH =
        "(a)-->(b)-->(c)-->(d)-->(a)";

    private static Graph expectedWithAggregation(Projection projection) {
        return fromGdl(EXPECTED_WITH_AGGREGATION, projection);
    }

    private static Graph expectedWithoutAggregation(Projection projection) {
        return fromGdl(EXPECTED_WITHOUT_AGGREGATION_GRAPH, projection);
    }

    private static Graph expectedUnweighted(Projection projection) {
        return fromGdl(EXPECTED_UNWEIGHTED_GRAPH, projection);
    }

    static Stream<Direction> validDirections() {
        return Stream.of(Direction.OUTGOING, Direction.INCOMING);
    }

    static Projection toProjection(Direction direction) {
        return direction == Direction.OUTGOING ? Projection.NATURAL : Projection.REVERSE;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validDirections")
    void unweighted(Direction direction) {
        int nodeCount = 4;
        GraphGenerator.NodeImporter nodeImporter = GraphGenerator.createNodeImporter(
            nodeCount,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        for (int i = 0; i < nodeCount; i++) {
            nodeImporter.addNode(i);
        }

        GraphGenerator.RelImporter relImporter = GraphGenerator.createRelImporter(
            nodeImporter,
            direction,
            false,
            false,
            Aggregation.SUM,
            false
        );

        for (int i = 0; i < nodeCount; i++) {
            relImporter.add(i, (i + 1) % nodeCount);
        }
        Graph graph = relImporter.buildGraph();
        assertGraphEquals(expectedUnweighted(toProjection(direction)), graph);
        assertEquals(nodeCount, graph.relationshipCount());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validDirections")
    void weightedWithAggregation(Direction direction) {
        Graph graph = generateGraph(direction, false, Aggregation.SUM);
        assertGraphEquals(expectedWithAggregation(toProjection(direction)), graph);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validDirections")
    void weightedWithoutAggregation(Direction direction) {
        Graph graph = generateGraph(direction, false, Aggregation.NONE);
        assertGraphEquals(expectedWithoutAggregation(toProjection(direction)), graph);
    }

    @Test
    void undirectedWithAggregation() {
        Graph graph = generateGraph(Direction.OUTGOING, true, Aggregation.SUM);
        assertGraphEquals(expectedWithAggregation(Projection.UNDIRECTED), graph);
    }

    @Test
    void undirectedWithoutAggregation() {
        Graph graph = generateGraph(Direction.OUTGOING, true, Aggregation.NONE);
        assertGraphEquals(expectedWithoutAggregation(Projection.UNDIRECTED), graph);
    }

    @Test
    void shouldFailOnIncomingWithUndirected() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> generateGraph(Direction.INCOMING, true, Aggregation.SUM)
        );
        assertTrue(exception.getMessage().contains("Direction must be OUTGOING if graph is undirected"));
    }

    private Graph generateGraph(Direction outgoing, boolean undirected, Aggregation aggregation) {
        int nodeCount = 4;

        GraphGenerator.NodeImporter nodeImporter = GraphGenerator.createNodeImporter(
            nodeCount,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        for (int i = 0; i < nodeCount; i++) {
            nodeImporter.addNode(i);
        }

        GraphGenerator.RelImporter relImporter = GraphGenerator.createRelImporter(
            nodeImporter,
            outgoing,
            undirected,
            true,
            aggregation,
            false
        );

        for (int i = 0; i < nodeCount; i++) {
            relImporter.add(i, (i + 1) % nodeCount, i);
            relImporter.add(i, (i + 1) % nodeCount, i);
        }
        return relImporter.buildGraph();
    }
}
