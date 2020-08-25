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
package org.neo4j.graphalgo.beta.pregel.bfs;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class BFSPregelAlgoTest {

    @GdlGraph
    private static final String TEST_GRAPH =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (i:Node)" +
        ", (a)-[:TYPE]->(b)" +
        ", (a)-[:TYPE]->(c)" +
        ", (b)-[:TYPE]->(d)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(e)" +
        ", (d)-[:TYPE]->(f)" +
        ", (e)-[:TYPE]->(g)" +
        ", (f)-[:TYPE]->(g)";

    @Inject
    private TestGraph graph;

    @Test
    void levelBfs() {
        int maxIterations = 10;

        var config = ImmutableBFSPregelConfig.builder()
            .maxIterations(maxIterations)
            .startNode(0)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new BFSLevelPregel(),
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        var result = pregelJob.run();

        assertTrue(result.didConverge(), "Algorithm did not converge.");
        assertEquals(5, result.ranIterations());

       var expected = Map.of(
            "a", 0L,
            "b", 1L,
            "c", 1L,
            "d", 2L,
            "e", 3L,
            "f", 3L,
            "g", 4L,
            "i", -1L
        );

        TestSupport.assertLongValues(graph, (nodeId) -> result.nodeValues().longValue(BFSLevelPregel.LEVEL,nodeId), expected);
    }

    @Test
    void parentBfs() {
        int maxIterations = 10;

        var config = ImmutableBFSPregelConfig.builder()
            .maxIterations(maxIterations)
            .startNode(0)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new BFSParentPregel(),
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        var result = pregelJob.run();

        assertTrue(result.didConverge(), "Algorithm did not converge.");
        assertEquals(4, result.ranIterations());

        var expected = Map.of(
            "a", 0L,
            "b", 0L,
            "c", 0L,
            "d", 1L,
            "e", 3L,
            "f", 3L,
            "g", 4L,
            "i", Long.MAX_VALUE
        );

        TestSupport.assertLongValues(graph, (nodeId) -> result.nodeValues().longValue(BFSParentPregel.PARENT,nodeId), expected);
    }
}
