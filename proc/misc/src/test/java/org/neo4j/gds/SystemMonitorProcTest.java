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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.gds.test.TestAlgorithm;
import org.neo4j.gds.test.TestConfig;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThan;

class SystemMonitorProcTest extends BaseProgressTest {

    private static final String GRAPH_NAME = "myGraph";
    private static final long NODE_COUNT = 3;
    private static final long MEMORY_RANGE_SIZE = 10;

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

        @Procedure("gds.test.algoTestProc")
        public Stream<Bar> foo(
            @Name(value = "graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            this.taskRegistryFactory = () -> new NonReleasingTaskRegistry(taskRegistryFactory.newInstance());
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
                    Graph graph, TestConfig configuration, AllocationTracker allocationTracker, ProgressTracker progressTracker
                ) {
                    return new TestAlgorithm(
                        graph,
                        allocationTracker,
                        0L,
                        progressTracker,
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

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 99999999})
    @GdsEditionTest(Edition.EE)
    void shouldSetResourceEstimationOnBaseTask(int concurrency) {
        runQuery(
            "CALL gds.test.algoTestProc($graph, { writeProperty: $writeProperty, concurrency: $concurrency })",
            Map.of(
                "graph", GRAPH_NAME,
                "writeProperty", "dummy",
                "concurrency", concurrency
            )
        );

        runQueryWithRowConsumer(
            "CALL gds.alpha.systemMonitor()",
            row -> {
                assertThat(row.getNumber("freeHeap").longValue()).isGreaterThan(0L);
                assertThat(row.getNumber("totalHeap").longValue()).isGreaterThan(0L);
                assertThat(row.getNumber("maxHeap").longValue()).isGreaterThan(0L);

                long jvmAvailableCpuCores = row.getNumber("jvmAvailableCpuCores").longValue();
                assertThat(jvmAvailableCpuCores).isGreaterThan(0L);
                long availableCpuCoresNotRequested = row.getNumber("availableCpuCoresNotRequested").longValue();
                assertThat(availableCpuCoresNotRequested).isEqualTo(jvmAvailableCpuCores - concurrency);

                assertThat(row.get("jvmHeapStatus")).satisfies(map -> {
                    assertThat(map).isInstanceOf(Map.class);
                    assertThat((Map) map).containsOnlyKeys("freeHeap", "totalHeap", "maxHeap");
                });

                assertThat(row.get("ongoingGdsProcedures"))
                    .isInstanceOf(List.class)
                    .asList()
                    .containsExactlyInAnyOrder(
                        Map.of(
                            "procedure", "TestAlgorithm",
                            "progress", "100%",
                            "estimatedMemoryRange", MemoryRange.of(NODE_COUNT * MEMORY_RANGE_SIZE).toString(),
                            "requestedNumberOfCpuCores", String.valueOf(concurrency)
                        )
                    );
            }
        );
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void shouldShowOngoingProcsOfSeveralUsers() {
        runQuery("Alice", "CALL gds.test.pl('foo')");
        // Use a non-default mock memory estimation.
        runQuery("Bob", "CALL gds.test.pl('bar', true, true)");

        runQueryWithRowConsumer(
            "CALL gds.alpha.systemMonitor()",
            row -> {
                assertThat(row.getNumber("freeHeap").longValue()).isGreaterThan(0L);
                assertThat(row.getNumber("totalHeap").longValue()).isGreaterThan(0L);
                assertThat(row.getNumber("maxHeap").longValue()).isGreaterThan(0L);

                long jvmAvailableCpuCores = row.getNumber("jvmAvailableCpuCores").longValue();
                assertThat(jvmAvailableCpuCores).isGreaterThan(0L);
                long availableCpuCoresNotRequested = row.getNumber("availableCpuCoresNotRequested").longValue();
                assertThat(availableCpuCoresNotRequested).isEqualTo(jvmAvailableCpuCores - REQUESTED_CPU_CORES);

                assertThat(row.get("jvmHeapStatus")).satisfies(map -> {
                    assertThat(map).isInstanceOf(Map.class);
                    assertThat((Map) map).containsOnlyKeys("freeHeap", "totalHeap", "maxHeap");
                });

                assertThat(row.get("ongoingGdsProcedures"))
                    .isInstanceOf(List.class)
                    .asList()
                    .containsExactlyInAnyOrder(
                        Map.of(
                            "procedure", "foo",
                            "progress", "33.33%",
                            "estimatedMemoryRange", "n/a",
                            "requestedNumberOfCpuCores", "n/a"
                        ),
                        Map.of(
                            "procedure", "bar",
                            "progress", "33.33%",
                            "estimatedMemoryRange", MEMORY_ESTIMATION_RANGE.toString(),
                            "requestedNumberOfCpuCores", String.valueOf(REQUESTED_CPU_CORES)
                        )
                    );
            }
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
                "jvmAvailableCpuCores", greaterThan(0L),
                "availableCpuCoresNotRequested", greaterThan(0L),
                "jvmHeapStatus", aMapWithSize(3),
                "ongoingGdsProcedures", Matchers.empty()
            ))
        );
    }

    @Test
    @GdsEditionTest(Edition.CE)
    void shouldFailOnCommunityEdition() {
        assertThatThrownBy(() -> runQuery("CALL gds.alpha.systemMonitor()"))
            .hasRootCauseInstanceOf(RuntimeException.class)
            .hasMessageContaining("Neo4j Graph Data Science library Enterprise Edition")
            .hasMessageContaining("System monitoring");
    }
}
