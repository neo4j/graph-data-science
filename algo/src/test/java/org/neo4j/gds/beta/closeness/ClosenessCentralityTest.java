/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.beta.closeness;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

/**
 * Graph:
 *
 * (A)<-->(B)<-->(C)<-->(D)<-->(E)
 *
 * Calculation:
 *
 * N = 5        // number of nodes
 * k = N-1 = 4  // used for normalization
 *
 * A     B     C     D     E
 * --|-----------------------------
 * A | 0     1     2     3     4       // farness between each pair of nodes
 * B | 1     0     1     2     3
 * C | 2     1     0     1     2
 * D | 3     2     1     0     1
 * E | 4     3     2     1     0
 * --|-----------------------------
 * S | 10    7     6     7     10      // sum each column
 * ==|=============================
 * k/S| 0.4  0.57  0.67  0.57   0.4     // normalized centrality
 */
@GdlExtension
class ClosenessCentralityTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +

        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE]->(a)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(b)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(c)" +
        ", (d)-[:TYPE]->(e)" +
        ", (e)-[:TYPE]->(d)";

    @Inject
    private TestGraph graph;

    @Test
    void testGetCentrality() {
        IdFunction idFunction = graph::toMappedNodeId;

        var algo = ClosenessCentrality.of(
            graph,
            ImmutableClosenessCentralityStreamConfig.builder().build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = algo.compute().centralities();

        assertThat(result.get(idFunction.of("a"))).isCloseTo(0.4, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("b"))).isCloseTo(0.57, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("c"))).isCloseTo(0.66, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("d"))).isCloseTo(0.57, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("e"))).isCloseTo(0.4, Offset.offset(0.01));
    }

    @Test
    void shouldLogProgress() {
        var config = ImmutableClosenessCentralityStreamConfig.builder().concurrency(4).build();
        var progressTask = new ClosenessCentralityFactory<>().progressTask(graph, config);
        var testLog = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, testLog, 1, EmptyTaskRegistryFactory.INSTANCE);

        var algo = ClosenessCentrality.of(
            graph,
            config,
            Pools.DEFAULT,
            progressTracker
        );

        algo.compute();

        List<AtomicLong> progresses = progressTracker.getProgresses();
        assertEquals(3, progresses.size());

        var messagesInOrder = testLog.getMessages(INFO);

        assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .hasSize(8)
            .containsSequence(
                "ClosenessCentrality :: Start",
                "ClosenessCentrality :: Farness computation :: Start",
                "ClosenessCentrality :: Farness computation 100%",
                "ClosenessCentrality :: Farness computation :: Finished",
                "ClosenessCentrality :: Closeness computation :: Start",
                "ClosenessCentrality :: Closeness computation 100%",
                "ClosenessCentrality :: Closeness computation :: Finished",
                "ClosenessCentrality :: Finished"
            );
    }
}
