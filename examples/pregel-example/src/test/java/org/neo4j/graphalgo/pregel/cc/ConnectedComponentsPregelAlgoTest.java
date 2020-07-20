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
package org.neo4j.graphalgo.pregel.cc;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.beta.pregel.ImmutablePregelConfig;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.core.ExceptionMessageMatcher.containsMessage;

@GdlExtension
class ConnectedComponentsPregelAlgoTest {

    @GdlGraph(graphNamePrefix = "directed")
    private static final String TEST_GRAPH =
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (e:Node)" +
            ", (f:Node)" +
            ", (g:Node)" +
            ", (h:Node)" +
            ", (i:Node)" +
            // {J}
            ", (j:Node { id: 9 })" +
            // {A, B, C, D}
            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(d)" +
            ", (d)-[:TYPE]->(a)" +
            // {E, F, G}
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(g)" +
            ", (g)-[:TYPE]->(e)" +
            // {H, I}
            ", (i)-[:TYPE]->(h)" +
            ", (h)-[:TYPE]->(i)";

    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED)
    private static final String TEST_GRAPH_UNDIRECTED = TEST_GRAPH;

    @Inject
    private TestGraph directedGraph;

    @Inject
    private TestGraph undirectedGraph;

    @Test
    void directedSCC() {
        int batchSize = 10;
        int maxIterations = 10;

        ConnectedComponentsConfig config = ImmutableConnectedComponentsConfig.builder()
            .maxIterations(maxIterations)
            .build();

        Pregel pregelJob = Pregel.withDefaultNodeValues(
            directedGraph,
            config,
            new ConnectedComponentsPregel(),
            batchSize,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        HugeDoubleArray nodeValues = pregelJob.run(maxIterations);

        var expected = new HashMap<String, Long>();
        expected.put("a", 0L);
        expected.put("b", 0L);
        expected.put("c", 0L);
        expected.put("d", 0L);
        expected.put("e", 4L);
        expected.put("f", 4L);
        expected.put("g", 4L);
        expected.put("h", 7L);
        expected.put("i", 7L);
        expected.put("j", 9L);

        TestSupport.assertLongValues(directedGraph, (nodeId) -> (long) nodeValues.get(nodeId), expected);
    }

    @Test
    void undirectedWCC() {
        int batchSize = 10;
        int maxIterations = 10;

        PregelConfig config = ImmutablePregelConfig.builder()
            .isAsynchronous(true)
            .concurrency(2)
            .maxIterations(maxIterations)
            .build();

        Pregel pregelJob = Pregel.withDefaultNodeValues(
            undirectedGraph,
            config,
            new ConnectedComponentsPregel(),
            batchSize,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        HugeDoubleArray nodeValues = pregelJob.run(maxIterations);

        var expected = new HashMap<String, Long>();
        expected.put("a", 0L);
        expected.put("b", 0L);
        expected.put("c", 0L);
        expected.put("d", 0L);
        expected.put("e", 4L);
        expected.put("f", 4L);
        expected.put("g", 4L);
        expected.put("h", 7L);
        expected.put("i", 7L);
        expected.put("j", 9L);

        TestSupport.assertLongValues(undirectedGraph, (nodeId) -> (long) nodeValues.get(nodeId), expected);
    }

    @Test
    void shouldFailWithConcurrency10() {
        int maxIterations = 10;

        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> {
            PregelConfig config = ImmutableConnectedComponentsConfig.builder()
                .concurrency(10)
                .maxIterations(maxIterations)
                .build();
        });

        assertThat(illegalArgumentException, containsMessage("The configured `concurrency` value is too high"));
    }
}
