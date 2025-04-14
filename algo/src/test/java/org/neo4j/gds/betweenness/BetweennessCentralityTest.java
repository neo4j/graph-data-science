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
package org.neo4j.gds.betweenness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.CentralityAlgorithmTasks;
import org.neo4j.gds.TestProgressTrackerHelper;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

class BetweennessCentralityTest {

    private static final String DIAMOND =
        "CREATE " +
        "  (a1)-[:REL]->(b)" +
        ", (a2)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (b)-[:REL]->(d)" +
        ", (c)-[:REL]->(e)" +
        ", (d)-[:REL]->(e)" +
        ", (e)-[:REL]->(f)";

    private static final String LINE =
        "CREATE" +
        "  (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(d)" +
        ", (d)-[:REL]->(e)";

    private static final String CYCLE =
        "CREATE" +
        "  (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)";

    private static final String CLIQUE_5 =
        "CREATE" +
        "  (a)-[:REL]->(b)" +
        "  (a)-[:REL]->(c)" +
        "  (a)-[:REL]->(d)" +
        "  (a)-[:REL]->(e)" +
        ", (b)-[:REL]->(c)" +
        ", (b)-[:REL]->(d)" +
        ", (b)-[:REL]->(e)" +
        ", (c)-[:REL]->(d)" +
        ", (c)-[:REL]->(e)" +
        ", (d)-[:REL]->(e)";

    private static final String DISCONNECTED_CYCLES =
        "CREATE" +
        // Cycle 1
        "  (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        // Cycle 2
        ", (d)-[:REL]->(e)" +
        ", (e)-[:REL]->(f)" +
        ", (f)-[:REL]->(d)";

    private static final String CONNECTED_CYCLES =
        "CREATE" +
        // Cycle 1
        "  (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        // Cycle 2
        ", (d)-[:REL]->(e)" +
        ", (e)-[:REL]->(f)" +
        ", (f)-[:REL]->(d)" +
        // Connection
        ", (a)-[:REL]->(d)" +
        ", (d)-[:REL]->(a)";

    private static Stream<Arguments> testArguments() {
        return crossArguments(() -> Stream.of(1, 4).map(Arguments::of), BetweennessCentralityTest::expectedResults);
    }

    private static Stream<Arguments> expectedResults() {
        return Stream.of(
            Arguments.of(fromGdl(LINE, "line"), 5, Map.of("a", 0.0, "b", 3.0, "c", 4.0, "d", 3.0, "e", 0.0)),
            Arguments.of(fromGdl(LINE, "line"), 2, Map.of("a", 0.0, "b", 3.0, "c", 4.0, "d", 2.0, "e", 0.0)),
            Arguments.of(fromGdl(LINE, "line"), 0, Map.of("a", 0.0, "b", 0.0, "c", 0.0, "d", 0.0, "e", 0.0)),
            Arguments.of(fromGdl(CYCLE, "cycle"), 3, Map.of("a", 1.0, "b", 1.0, "c", 1.0)),
            Arguments.of(fromGdl(CLIQUE_5, "clique_5"), 5, Map.of("a", 0.0, "b", 0.0, "c", 0.0, "d", 0.0, "e", 0.0)),
            Arguments.of(fromGdl(CLIQUE_5, UNDIRECTED, "undirected_clique_5"), 5, Map.of("a", 0.0, "b", 0.0, "c", 0.0, "d", 0.0, "e", 0.0)),
            Arguments.of(fromGdl(CLIQUE_5, UNDIRECTED,"undirected_clique_5"), 3, Map.of("a", 0.0, "b", 0.0, "c", 0.0, "d", 0.0, "e", 0.0)),
            Arguments.of(fromGdl(DISCONNECTED_CYCLES, "disconnected_cycles"), 6, Map.of("a", 1.0, "b", 1.0, "c", 1.0, "d", 1.0, "e", 1.0, "f", 1.0)),
            Arguments.of(fromGdl(CONNECTED_CYCLES, "connected_cycles"), 6, Map.of("a", 13.0, "b", 4.0, "c", 4.0, "d", 13.0, "e", 4.0, "f", 4.0)),
            Arguments.of(fromGdl(CONNECTED_CYCLES, "connected_cycles"), 2, Map.of("a", 4.0, "b", 1.0, "c", 0.0, "d", 4.0, "e", 2.0, "f", 0.0)),
            Arguments.of(fromGdl(DIAMOND, "diamond"), 7, Map.of("a1", 0.0, "a2", 0.0, "b", 8.0, "c", 3.0, "d", 3.0, "e", 5.0, "f", 0.0)),
            Arguments.of(fromGdl(DIAMOND, UNDIRECTED, "undirected_diamond"), 7, Map.of("a1", 0.0, "a2", 0.0, "b", 9.5, "c", 3.0, "d", 3.0, "e", 5.5, "f", 0.0))
        );
    }

    @ParameterizedTest(name = "graph={1}, concurrency={0}, samplingSize={2}")
    @MethodSource("org.neo4j.gds.betweenness.BetweennessCentralityTest#testArguments")
    void sampling(int concurrency, TestGraph graph, long samplingSize, Map<String, Double> expectedResult) {

       var params = new BetweennessCentralityParameters(
           new Concurrency(concurrency),
           Optional.of(samplingSize),
           Optional.of(42L),
           false
       );

        var algorithm = BetweennessCentrality.create(
            graph,
            params,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var actualResult = algorithm.compute().centralities();
        assertEquals(expectedResult.size(), actualResult.size());
        expectedResult.forEach((variable, expectedCentrality) ->
            assertEquals(expectedCentrality, actualResult.get(graph.toMappedNodeId(variable)), variable)
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void noSampling(int concurrency) {
        TestGraph graph = fromGdl(LINE);

        var params = new BetweennessCentralityParameters(
            new Concurrency(concurrency),
            Optional.empty(),
            Optional.empty(),
            false
        );

        var algorithm = BetweennessCentrality.create(
            graph,
            params,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var actualResult = algorithm.compute().centralities();

        assertEquals(5, actualResult.size(), "Expected 5 centrality values");
        assertEquals(0.0, actualResult.get((int) graph.toMappedNodeId("a")));
        assertEquals(3.0, actualResult.get((int) graph.toMappedNodeId("b")));
        assertEquals(4.0, actualResult.get((int) graph.toMappedNodeId("c")));
        assertEquals(3.0, actualResult.get((int) graph.toMappedNodeId("d")));
        assertEquals(0.0, actualResult.get((int) graph.toMappedNodeId("e")));
    }

    @Test
    void testShouldLogProgress() {
        var testGraph = fromGdl(DIAMOND, "diamond");

        var parameters = new BetweennessCentralityParameters(
            new Concurrency(4),
            Optional.of(2L),
            Optional.empty(),
            false
        );

        var task = new CentralityAlgorithmTasks().betweennessCentrality(testGraph,parameters);
        var progressTrackerWithLog = TestProgressTrackerHelper.create(
            task,
            new Concurrency(4)
        );
        var progressTracker = progressTrackerWithLog.progressTracker();
        var log = progressTrackerWithLog.log();

        var algorithm = BetweennessCentrality.create(
            testGraph,
            parameters,
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        );

        algorithm.compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "Betweenness Centrality :: Start",
                "Betweenness Centrality 50%",
                "Betweenness Centrality 100%",
                "Betweenness Centrality :: Finished"
            );
    }

    @Test
    void testShouldLogProgressNoSampling() {
        var testGraph = fromGdl(DIAMOND, "diamond");

        var parameters = new BetweennessCentralityParameters(
            new Concurrency(4),
            Optional.empty(),
            Optional.empty(),
            false
        );
        var task = new CentralityAlgorithmTasks().betweennessCentrality(testGraph,parameters);

        var progressTrackerWithLog = TestProgressTrackerHelper.create(
            task,
            new Concurrency(4)
        );
        var progressTracker = progressTrackerWithLog.progressTracker();
        var log = progressTrackerWithLog.log();

        var algorithm = BetweennessCentrality.create(
            testGraph,
            parameters,
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        );

        algorithm.compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "Betweenness Centrality :: Start",
                "Betweenness Centrality 14%",
                "Betweenness Centrality 28%",
                "Betweenness Centrality 42%",
                "Betweenness Centrality 57%",
                "Betweenness Centrality 71%",
                "Betweenness Centrality 85%",
                "Betweenness Centrality 100%",
                "Betweenness Centrality :: Finished"
            );
    }

}
