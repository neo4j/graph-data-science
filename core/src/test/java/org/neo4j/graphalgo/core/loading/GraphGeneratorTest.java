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
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private static Graph expectedWithAggregation(Orientation orientation) {
        return fromGdl(EXPECTED_WITH_AGGREGATION, orientation);
    }

    private static Graph expectedWithoutAggregation(Orientation orientation) {
        return fromGdl(EXPECTED_WITHOUT_AGGREGATION_GRAPH, orientation);
    }

    private static Graph expectedUnweighted(Orientation orientation) {
        return fromGdl(EXPECTED_UNWEIGHTED_GRAPH, orientation);
    }

    static Stream<Orientation> validProjections() {
        return Stream.of(Orientation.NATURAL, Orientation.REVERSE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validProjections")
    void unweighted(Orientation orientation) {
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
            orientation,
            false,
            Aggregation.SUM
        );

        for (int i = 0; i < nodeCount; i++) {
            relImporter.add(i, (i + 1) % nodeCount);
        }
        Graph graph = relImporter.buildGraph();
        assertGraphEquals(expectedUnweighted(orientation), graph);
        assertEquals(nodeCount, graph.relationshipCount());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validProjections")
    void weightedWithAggregation(Orientation orientation) {
        Graph graph = generateGraph(orientation, Aggregation.SUM);
        assertGraphEquals(expectedWithAggregation(orientation), graph);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validProjections")
    void weightedWithoutAggregation(Orientation orientation) {
        Graph graph = generateGraph(orientation, Aggregation.NONE);
        assertGraphEquals(expectedWithoutAggregation(orientation), graph);
    }

    @Test
    void undirectedWithAggregation() {
        Graph graph = generateGraph(Orientation.UNDIRECTED, Aggregation.SUM);
        assertGraphEquals(expectedWithAggregation(Orientation.UNDIRECTED), graph);
    }

    @Test
    void undirectedWithoutAggregation() {
        Graph graph = generateGraph(Orientation.UNDIRECTED, Aggregation.NONE);
        assertGraphEquals(expectedWithoutAggregation(Orientation.UNDIRECTED), graph);
    }

    private Graph generateGraph(Orientation orientation, Aggregation aggregation) {
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
            orientation,
            true,
            aggregation
        );

        for (int i = 0; i < nodeCount; i++) {
            relImporter.add(i, (i + 1) % nodeCount, i);
            relImporter.add(i, (i + 1) % nodeCount, i);
        }
        return relImporter.buildGraph();
    }
}
