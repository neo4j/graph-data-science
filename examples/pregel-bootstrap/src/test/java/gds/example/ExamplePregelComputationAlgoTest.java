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
package gds.example;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.HashMap;

import static gds.example.ExamplePregelComputation.KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class ExamplePregelComputationAlgoTest {

    @GdlGraph
    private static final String MY_TEST_GRAPH =
        "CREATE" +
        "  (alice)" +
        ", (bob)" +
        ", (eve)" +
        ", (alice)-[:LIKES]->(bob)" +
        ", (bob)-[:LIKES]->(alice)" +
        ", (eve)-[:DISLIKES]->(alice)" +
        ", (eve)-[:DISLIKES]->(bob)";

    @Inject
    private TestGraph graph;
    
    @Test
    void runExamplePregelComputation() {
        int maxIterations = 10;

        var config = ImmutableExampleConfig.builder()
            .maxIterations(maxIterations)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new ExamplePregelComputation(),
            Pools.DEFAULT,
            AllocationTracker.empty(),
            ProgressTracker.NULL_TRACKER
        );

        var result = pregelJob.run();

        assertTrue(result.didConverge(), "Algorithm did not converge.");
        assertEquals(0, result.ranIterations());

        var expected = new HashMap<String, Long>();
        expected.put("alice", 0L);
        expected.put("bob", 1L);
        expected.put("eve", 2L);

        TestSupport.assertLongValues(graph, (nodeId) -> result.nodeValues().longValue(KEY, nodeId), expected);
    }
}
