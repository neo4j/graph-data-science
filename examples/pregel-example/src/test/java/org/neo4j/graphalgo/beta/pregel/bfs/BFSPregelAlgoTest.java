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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class BFSPregelAlgoTest {

    /**
     * Graph:
     *         (i)
     *     (b)   (e)
     *    /  \  /  \
     *  (a)  (d)   (g)
     *    \  /  \  /
     *    (c)   (f)
     */
    @GdlGraph
    private static final String TEST_GRAPH =
        " (a), (b), (c), (d), (e), (f), (g), (i)" +
        " (a)-->(b)," +
        " (a)-->(c)," +
        " (b)-->(d)," +
        " (c)-->(d)," +
        " (d)-->(e)," +
        " (d)-->(f)," +
        " (e)-->(g)," +
        " (f)-->(g)";;

    @Inject
    private TestGraph graph;

    @Test
    void levelBfs() {
        int batchSize = 10;
        int maxIterations = 10;

        var config = ImmutableBFSPregelConfig.builder()
            .maxIterations(maxIterations)
            .startNode(0)
            .build();

        var pregelJob = Pregel.withDefaultNodeValues(
            graph,
            config,
            new BFSLevelPregel(),
            batchSize,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        var result = pregelJob.run();

        assertTrue(result.didConverge(), "Algorithm did not converge.");
        assertEquals(5, result.ranIterations());

        var expected = new HashMap<String, Long>();
        expected.put("a", 0L);
        expected.put("b", 1L);
        expected.put("c", 1L);
        expected.put("d", 2L);
        expected.put("e", 3L);
        expected.put("f", 3L);
        expected.put("g", 4L);
        expected.put("i", -1L);

        TestSupport.assertLongValues(graph, (nodeId) -> (long) result.nodeValues().get(nodeId), expected);
    }

    @Test
    void parentBfs() {
        int batchSize = 10;
        int maxIterations = 10;

        var config = ImmutableBFSPregelConfig.builder()
            .maxIterations(maxIterations)
            .startNode(0)
            .build();

        var pregelJob = Pregel.withDefaultNodeValues(
            graph,
            config,
            new BFSParentPregel(),
            batchSize,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        var result = pregelJob.run();

        assertTrue(result.didConverge(), "Algorithm did not converge.");
        assertEquals(5, result.ranIterations());

        var expected = new HashMap<String, Double>();
        expected.put("a", 0D);
        expected.put("b", 0D);
        expected.put("c", 0D);
        expected.put("d", 1D);
        expected.put("e", 3D);
        expected.put("f", 3D);
        expected.put("g", 4D);
        expected.put("i", Double.MAX_VALUE);

        TestSupport.assertDoubleValues(graph, (nodeId) -> result.nodeValues().get(nodeId), expected, 0);
    }
}
