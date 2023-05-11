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
package org.neo4j.gds.beta.pregel.cc;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.beta.pregel.cc.ConnectedComponentsPregel.COMPONENT;
import static org.neo4j.gds.core.ExceptionMessageMatcher.containsMessage;

@GdlExtension
class ConnectedComponentsPregelAlgoTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
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
            ", (j:Node)" +
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

    @Inject
    private TestGraph graph;

    @Test
    void wcc() {
        int maxIterations = 10;

        var config = ImmutableConnectedComponentsConfig.builder()
            .concurrency(2)
            .maxIterations(maxIterations)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new ConnectedComponentsPregel(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = pregelJob.run();

        assertTrue(result.didConverge(), "Algorithm did not converge.");
        assertEquals(1, result.ranIterations());

        var expected = new HashMap<String, Long>();
        expected.put("a", 3L);
        expected.put("b", 3L);
        expected.put("c", 3L);
        expected.put("d", 3L);
        expected.put("e", 7L);
        expected.put("f", 7L);
        expected.put("g", 7L);
        expected.put("h", 0L);
        expected.put("i", 0L);
        expected.put("j", 2L);

        TestSupport.assertLongValues(graph, (nodeId) -> result.nodeValues().longValue(COMPONENT, nodeId), expected);
    }

    @Test
    void shouldFailWithConcurrency10() {
        int maxIterations = 10;

        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> {
            ImmutableConnectedComponentsConfig.builder()
                .concurrency(10)
                .maxIterations(maxIterations)
                .build();
        });

        assertThat(illegalArgumentException, containsMessage("Community users cannot exceed writeConcurrency=4 (you configured writeConcurrency=10), see https://neo4j.com/docs/graph-data-science/"));
    }
}
