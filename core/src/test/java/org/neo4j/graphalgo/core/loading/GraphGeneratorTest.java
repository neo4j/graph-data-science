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

package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class GraphGeneratorTest {

    public static final Graph EXPECTED_WITH_DEDUPLICATION = fromGdl("(a)-[{w: 0.0}]->(b)-[{w: 2.0}]->(c)-[{w: 4.0}]->(d)-[{w: 6.0}]->(a)");
    public static final Graph EXPECTED_WITHOUT_DEDUPLICATION = fromGdl(
        "(a)-[{w: 0.0}]->(b)" +
        "(a)-[{w: 0.0}]->(b)" +
        "(b)-[{w: 1.0}]->(c)" +
        "(b)-[{w: 1.0}]->(c)" +
        "(c)-[{w: 2.0}]->(d)" +
        "(c)-[{w: 2.0}]->(d)" +
        "(d)-[{w: 3.0}]->(a)" +
        "(d)-[{w: 3.0}]->(a)"
    );
    public static final Graph EXPECTED_UNWEIGHTED = fromGdl("(a)-->(b)-->(c)-->(d)-->(a)");

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = Direction.class)
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
            DeduplicationStrategy.SUM
        );

        for (int i = 0; i < nodeCount; i++) {
            relImporter.add(i, (i + 1) % nodeCount);
        }
        Graph graph = relImporter.buildGraph();
        assertGraphEquals(EXPECTED_UNWEIGHTED, graph);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = Direction.class)
    void weightedWithDeduplication(Direction direction) {
        Graph graph = generateGraph(direction, false, DeduplicationStrategy.SUM);
        assertGraphEquals(EXPECTED_WITH_DEDUPLICATION, graph);
        assertEquals(direction, graph.getLoadDirection());
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = Direction.class)
    void weightedWithoutDeduplication(Direction direction) {
        Graph graph = generateGraph(direction, false, DeduplicationStrategy.NONE);
        assertGraphEquals(EXPECTED_WITHOUT_DEDUPLICATION, graph);
        assertEquals(direction, graph.getLoadDirection());
    }

    @Test
    void undirectedWithDeduplication() {
        Graph graph = generateGraph(Direction.OUTGOING, true, DeduplicationStrategy.SUM);
        assertGraphEquals(EXPECTED_WITH_DEDUPLICATION, graph);
        assertEquals(Direction.OUTGOING, graph.getLoadDirection());
    }

    @Test
    void undirectedWithoutDeduplication() {
        Graph graph = generateGraph(Direction.OUTGOING, true, DeduplicationStrategy.NONE);
        assertGraphEquals(EXPECTED_WITHOUT_DEDUPLICATION, graph);
        assertEquals(Direction.OUTGOING, graph.getLoadDirection());
    }

    @Test
    void shouldFailOnIncomingWithUndirected() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> generateGraph(Direction.INCOMING, true, DeduplicationStrategy.SUM)
        );
        assertTrue(exception.getMessage().contains("Direction must be OUTGOING if graph is undirected"));
    }

    private Graph generateGraph(Direction outgoing, boolean undirected, DeduplicationStrategy  deduplicationStrategy) {
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
            deduplicationStrategy
        );

        for (int i = 0; i < nodeCount; i++) {
            relImporter.add(i, (i + 1) % nodeCount, i);
            relImporter.add(i, (i + 1) % nodeCount, i);
        }
        return relImporter.buildGraph();
    }
}
