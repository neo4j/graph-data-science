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
package org.neo4j.gds.impl.scc;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class SccTest {

    @GdlGraph
    private static final String DB_CYPHER =
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

        ", (a)-[:TYPE {cost: 5}]->(b)" +
        ", (b)-[:TYPE {cost: 5}]->(c)" +
        ", (c)-[:TYPE {cost: 5}]->(a)" +

        ", (d)-[:TYPE {cost: 2}]->(e)" +
        ", (e)-[:TYPE {cost: 2}]->(f)" +
        ", (f)-[:TYPE {cost: 2}]->(d)" +

        ", (a)-[:TYPE {cost: 2}]->(d)" +

        ", (g)-[:TYPE {cost: 3}]->(h)" +
        ", (h)-[:TYPE {cost: 3}]->(i)" +
        ", (i)-[:TYPE {cost: 3}]->(g)";

    @Inject
    private TestGraph graph;

    @Test
    void testDirect() {
        Scc scc = new Scc(graph, ProgressTracker.NULL_TRACKER);
        HugeLongArray components = scc.compute();

        assertCC(components);

        HashMap<Long, Long> componentsMap = new HashMap<>();
        for (long nodeId = 0; nodeId < components.size(); ++nodeId) {
            long componentId = components.get(nodeId);
            long componentValue = componentsMap.getOrDefault(componentId, 0L);
            componentsMap.put(componentId, 1 + componentValue);
        }

        long max = 0;
        long min = Long.MAX_VALUE;
        for (var entry : componentsMap.entrySet()) {
            min = Math.min(entry.getValue(), min);
            max = Math.max(entry.getValue(), max);

        }

        assertThat(componentsMap.keySet().size()).isEqualTo(3L);
        assertThat(min).isEqualTo(3L);
        assertThat(max).isEqualTo(3L);

    }

    @Test
    void testHugeIterativeScc() {
        Scc algo = new Scc(graph, ProgressTracker.NULL_TRACKER);
        HugeLongArray components = algo.compute();
        assertCC(components);
    }

    private void assertCC(HugeLongArray connectedComponents) {
        IdFunction idFunction = graph::toMappedNodeId;

        assertBelongSameSet(connectedComponents,
            idFunction.of("a"),
            idFunction.of("b"),
            idFunction.of("c")
        );
        assertBelongSameSet(connectedComponents,
            idFunction.of("d"),
            idFunction.of("e"),
            idFunction.of("f")
        );
        assertBelongSameSet(connectedComponents,
            idFunction.of("g"),
            idFunction.of("h"),
            idFunction.of("i")
        );
    }

    // TODO: Try to get this working with AssertJ
    private void assertBelongSameSet(HugeLongArray data, Long... expected) {
        // check if all belong to same set
        final long needle = data.get(expected[0]);
        for (long l : expected) {
            assertThat(data.get(l)).isEqualTo(needle);
        }

        final List<Long> exp = Arrays.asList(expected);
        // check no other element belongs to this set
        for (long i = 0; i < data.size(); i++) {
            if (exp.contains(i)) {
                continue;
            }
            assertThat(data.get(i)).isNotEqualTo(needle);
        }
    }

    @Test
    void shouldLogProgress() {
        var config = SccStreamConfigImpl.builder().build();
        var factory = new SccAlgorithmFactory<>();
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            factory.progressTask(graph, config),
            log,
            4,
            EmptyTaskRegistryFactory.INSTANCE
        );
        factory.build(graph, config, progressTracker).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "Scc :: Start",
                "Scc 11%",
                "Scc 22%",
                "Scc 100%",
                "Scc :: Finished"
            );

    }
    
}
