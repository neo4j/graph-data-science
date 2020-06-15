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
package org.neo4j.graphalgo.betweenness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class SelectionStrategyTest {

    @GdlGraph
    private static final String DB_GDL =
        "(a)-->(b)" +
        "(a)-->(c)" +
        "(a)-->(d)" +
        "(a)-->(e)" +
        "(a)-->(f)" +
        "(a)-->(g)" +

        "(b)-->(h)" +
        "(b)-->(i)" +
        "(b)-->(j)" +
        "(b)-->(k)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction nodeId;

    @Test
    void selectAllNodes() {
        SelectionStrategy selectionStrategy = SelectionStrategy.ALL;
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(graph.nodeCount(), samplingSize(graph.nodeCount(), selectionStrategy));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 10, 11})
    void selectNumSeedNodes(long numSeedNodes) {
        SelectionStrategy selectionStrategy = new SelectionStrategy.RandomDegree(numSeedNodes);
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(numSeedNodes, samplingSize(graph.nodeCount(), selectionStrategy));
    }

    @Test
    void selectNumSeedNodesWithRandomSeed() {
        SelectionStrategy selectionStrategy = new SelectionStrategy.RandomDegree(3, Optional.of(42L));
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(3, samplingSize(graph.nodeCount(), selectionStrategy));
        assertTrue(selectionStrategy.select(nodeId.of("a")));
        assertTrue(selectionStrategy.select(nodeId.of("c")));
        assertTrue(selectionStrategy.select(nodeId.of("f")));
    }

    @Test
    void selectHighDegreeNode() {
        SelectionStrategy selectionStrategy = new SelectionStrategy.RandomDegree(1);
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(1, samplingSize(graph.nodeCount(), selectionStrategy));
        var isA = selectionStrategy.select(graph.toMappedNodeId(nodeId.of("a")));
        var isB = selectionStrategy.select(graph.toMappedNodeId(nodeId.of("b")));
        assertTrue(isA || isB);
    }

    private static long samplingSize(long nodeCount, SelectionStrategy selectionStrategy) {
        long samplingSize = 0;
        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            if (selectionStrategy.select(nodeId)) {
                samplingSize++;
            }
        }
        return samplingSize;
    }
}