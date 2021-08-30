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
package org.neo4j.gds;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.ProgressEventStore;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.gds.test.TestAlgorithm;
import org.neo4j.gds.test.TestConfig;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SystemMonitorProcTest extends BaseProgressTest {

    static String GRAPH_NAME = "myGraph";
    static long NODE_COUNT = 3;
    static long MEMORY_RANGE_SIZE = 10;

    @Context
    public ProgressEventStore progress;

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label1)" +
        ", (b:Label1)" +
        ", (c:Label1)" +
        ", (b)-[:TYPE1 {weight: 2.0}]->(c)";

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            BaseProgressTestProc.class,
            MemEstimationTestProc.class,
            GraphCreateProc.class,
            SystemMonitorProc.class
        );

        String createQuery = GdsCypher.call()
            .loadEverything()
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(createQuery);
    }

    public static class MemEstimationTestProc extends AlgoBaseProc<TestAlgorithm, TestAlgorithm, TestConfig> {
        @Context
        public ProgressEventTracker progress;

        @Procedure("gds.test.algoTestProc")
        public Stream<Bar> foo(
            @Name(value = "graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            compute(graphNameOrConfig, configuration);

            return Stream.empty();
        }

        @Override
        protected TestConfig newConfig(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
        ) {
            return TestConfig.of(username, graphName, maybeImplicitCreate, config);
        }

        @Override
        protected AlgorithmFactory<TestAlgorithm, TestConfig> algorithmFactory() {
            return new AlgorithmFactory<>() {

                @Override
                protected String taskName() {
                    return "TestAlgorithm";
                }

                @Override
                protected TestAlgorithm build(
                    Graph graph, TestConfig configuration, AllocationTracker tracker, ProgressTracker progressTracker
                ) {
                    return new TestAlgorithm(
                        graph,
                        allocationTracker(),
                        0L,
                        log,
                        new NonReleasingProgressTracker((TaskProgressTracker) progressTracker),
                        false
                    );
                }

                @Override
                public MemoryEstimation memoryEstimation(TestConfig configuration) {
                    return MemoryEstimations.builder()
                        .perNode("dummy", MemoryEstimations.of("fixed value", MemoryRange.of(MEMORY_RANGE_SIZE)))
                        .build();
                }
            };
        }
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void shouldSetMemoryEstimationOnBaseTask() {
        runQuery(
            "CALL gds.test.algoTestProc($graph, { writeProperty: $writeProperty })",
            Map.of("graph", GRAPH_NAME, "writeProperty", "dummy")
        );

        scheduler.forward(100, TimeUnit.MILLISECONDS);

        assertCypherResult(
            "CALL gds.alpha.systemMonitor()",
            List.of(Map.of(
                "freeHeap", greaterThan(0L),
                "totalHeap", greaterThan(0L),
                "maxHeap", greaterThan(0L),
                "jvmAvailableProcessors", greaterThan(0L),
                "jvmStatusDescription", aMapWithSize(4),
                "ongoingGdsProcedures", List.of(Map.of(
                    "taskName", "TestAlgorithm",
                    "progress", "n/a",
                    "maxMemoryEstimation", NODE_COUNT * MEMORY_RANGE_SIZE + " Bytes"
                ))
            ))
        );
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void shouldShowOngoingProcsOfSeveralUsers() {
        runQuery("Alice", "CALL gds.test.pl('foo')");
        // Use a non-default mock memory estimation.
        runQuery("Bob", "CALL gds.test.pl('bar', true)");

        assertCypherResult(
            "CALL gds.alpha.systemMonitor()",
            List.of(Map.of(
                "freeHeap", greaterThan(0L),
                "totalHeap", greaterThan(0L),
                "maxHeap", greaterThan(0L),
                "jvmAvailableProcessors", greaterThan(0L),
                "jvmStatusDescription", aMapWithSize(4),
                "ongoingGdsProcedures", containsInAnyOrder(
                    Map.of(
                        "taskName", "foo",
                        "progress", "33.33%",
                        "maxMemoryEstimation", "n/a"
                    ),
                    Map.of(
                        "taskName", "bar",
                        "progress", "33.33%",
                        "maxMemoryEstimation", MAX_MEMORY_USAGE + " Bytes"
                    )
                )
            ))
        );
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void shouldGiveSaneJvmStatus() {
        assertCypherResult(
            "CALL gds.alpha.systemMonitor()",
            List.of(Map.of(
                "freeHeap", greaterThan(0L),
                "totalHeap", greaterThan(0L),
                "maxHeap", greaterThan(0L),
                "jvmAvailableProcessors", greaterThan(0L),
                "jvmStatusDescription", aMapWithSize(4),
                "ongoingGdsProcedures", Matchers.empty()
            ))
        );
    }

    @Test
    @GdsEditionTest(Edition.CE)
    void shouldFailOnCommunityEdition() {
        assertThrows(RuntimeException.class, () -> runQuery("CALL gds.alpha.systemMonitor()"));
    }
}
