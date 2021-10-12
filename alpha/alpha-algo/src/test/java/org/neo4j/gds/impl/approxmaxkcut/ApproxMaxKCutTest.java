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
package org.neo4j.gds.impl.approxmaxkcut;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeAtomicByteArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
final class ApproxMaxKCutTest {

    // The optimal max cut for this graph when k = 2 is:
    //     {a, b, c}, {d, e, f, g} if the graph is unweighted.
    //     {a, c}, {b, d, e, f, g} if the graph is weighted.
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label1)" +
        ", (b:Label1)" +
        ", (c:Label1)" +
        ", (d:Label1)" +
        ", (e:Label1)" +
        ", (f:Label1)" +
        ", (g:Label1)" +

        ", (a)-[:TYPE1 {weight: 81.0}]->(b)" +
        ", (a)-[:TYPE1 {weight: 7.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(e)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(f)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(g)" +
        ", (c)-[:TYPE1 {weight: 45.0}]->(b)" +
        ", (c)-[:TYPE1 {weight: 3.0}]->(e)" +
        ", (d)-[:TYPE1 {weight: 3.0}]->(c)" +
        ", (d)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 3.0}]->(a)" +
        ", (f)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (g)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (g)-[:TYPE1 {weight: 4.0}]->(c)" +
        ", (g)-[:TYPE1 {weight: 999.0}]->(g)";

    @Inject
    private TestGraph graph;

    static Stream<Arguments> maxKCutParameters() {
        return TestSupport.crossArguments(
            () -> Stream.of(
                Arguments.of(
                    false,
                    Map.of("a", 0L, "b", 0L, "c", 0L, "d", 1L, "e", 1L, "f", 1L, "g", 1L),
                    13.0D
                ),
                Arguments.of(
                    true,
                    Map.of("a", 0L, "b", 1L, "c", 0L, "d", 1L, "e", 1L, "f", 1L, "g", 1L),
                    146.0D
                )
            ),
            () -> Stream.of(Arguments.of(0), Arguments.of(4)), // VNS max neighborhood order (0 means VNS not used)
            () -> Stream.of(Arguments.of(1), Arguments.of(4))  // concurrency
        );
    }

    @ParameterizedTest
    @MethodSource("maxKCutParameters")
    void computeCorrectResults(
        boolean weighted,
        Map<String, Long> expectedMapping,
        double expectedCost,
        int vnsMaxNeighborhoodOrder,
        int concurrency
    ) {
        var configBuilder = ImmutableApproxMaxKCutConfig.builder()
            .concurrency(concurrency)
            .k((byte) 2)
            .vnsMaxNeighborhoodOrder(vnsMaxNeighborhoodOrder)
            // We should not need as many iterations if we do VNS.
            .iterations(vnsMaxNeighborhoodOrder > 0 ? 100 : 25);

        if (weighted) {
            configBuilder.relationshipWeightProperty("weight");
        }

        if (concurrency > 1) {
            configBuilder.minBatchSize(1);
        }

        var config = configBuilder.build();

        var approxMaxKCut = new ApproxMaxKCut(
            graph,
            Pools.DEFAULT,
            config,
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        );

        var result = approxMaxKCut.compute();

        assertEquals(result.cutCost(), expectedCost);

        var setFunction = result.candidateSolution();

        expectedMapping.forEach((outerVar, outerExpectedSet) -> {
            long outerNodeId = graph.toMappedNodeId(outerVar);

            expectedMapping.forEach((innerVar, innerExpectedSet) -> {
                long innerNodeId = graph.toMappedNodeId(innerVar);

                if (outerExpectedSet.equals(innerExpectedSet)) {
                    assertEquals(setFunction.get(outerNodeId), setFunction.get(innerNodeId));
                } else {
                    assertNotEquals(setFunction.get(outerNodeId), setFunction.get(innerNodeId));
                }
            });
        });
    }

    @Test
    void invalidRandomParameters() {
        var configBuilder = ImmutableApproxMaxKCutConfig.builder()
            .concurrency(4)
            .randomSeed(1337L);
        assertThrows(IllegalArgumentException.class, configBuilder::build);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void progressLogging(int vnsMaxNeighborhoodOrder) {
        var configBuilder = ImmutableApproxMaxKCutConfig.builder();
        configBuilder.vnsMaxNeighborhoodOrder(vnsMaxNeighborhoodOrder);

        var config = configBuilder.build();

        var log = new TestLog();
        var approxMaxKCut = new ApproxMaxKCutFactory<>().build(
            graph,
            config,
            AllocationTracker.empty(),
            log,
            EmptyTaskRegistryFactory.INSTANCE
        );
        approxMaxKCut.compute();

        assertThat(log.containsMessage(TestLog.INFO, ":: Start")).isTrue();
        assertThat(log.containsMessage(TestLog.INFO, ":: Finish")).isTrue();

        for (int i = 1; i <= config.iterations(); i++) {
            assertThat(log.containsMessage(
                TestLog.INFO,
                formatWithLocale("place nodes randomly %s of %s :: Start", i, config.iterations())
            )).isTrue();
            assertThat(log.containsMessage(
                TestLog.INFO,
                formatWithLocale("place nodes randomly %s of %s 100%%", i, config.iterations())
            )).isTrue();
            assertThat(log.containsMessage(
                TestLog.INFO,
                formatWithLocale("place nodes randomly %s of %s :: Finished", i, config.iterations())
            )).isTrue();

            if (vnsMaxNeighborhoodOrder == 0) {
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale("local search %s of %s :: Start", i, config.iterations())
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale("local search %s of %s :: Finished", i, config.iterations())
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale("local search %s of %s :: improvement loop :: Start", i, config.iterations())
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale("local search %s of %s :: improvement loop :: Finished", i, config.iterations())
                )).isTrue();

                // May occur several times but we don't know.
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale(
                        "local search %s of %s :: improvement loop :: compute node to community weights 1 :: Start",
                        i,
                        config.iterations()
                    )
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale(
                        "local search %s of %s :: improvement loop :: compute node to community weights 1 100%%",
                        i,
                        config.iterations()
                    )
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale(
                        "local search %s of %s :: improvement loop :: compute node to community weights 1 :: Finished",
                        i,
                        config.iterations()
                    )
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale(
                        "local search %s of %s :: improvement loop :: swap for local improvements 1 :: Start",
                        i,
                        config.iterations()
                    )
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale(
                        "local search %s of %s :: improvement loop :: swap for local improvements 1 100%%",
                        i,
                        config.iterations()
                    )
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale(
                        "local search %s of %s :: improvement loop :: swap for local improvements 1 :: Finished",
                        i,
                        config.iterations()
                    )
                )).isTrue();

                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale(
                        "local search %s of %s :: compute current solution cost :: Start",
                        i,
                        config.iterations()
                    )
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale(
                        "local search %s of %s :: compute current solution cost 100%%",
                        i,
                        config.iterations()
                    )
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale(
                        "local search %s of %s :: compute current solution cost :: Finished",
                        i,
                        config.iterations()
                    )
                )).isTrue();
            } else {
                // We merely check that VNS is indeed run. The rest is very similar to the non-VNS case.
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale("variable neighborhood search %s of %s :: Start", i, config.iterations())
                )).isTrue();
                assertThat(log.containsMessage(
                    TestLog.INFO,
                    formatWithLocale("variable neighborhood search %s of %s :: Finished", i, config.iterations())
                )).isTrue();
            }
        }
    }

    static long memUsageHelper(long nodeCount, int k, boolean vns) {
        var memUsage = 0;

        memUsage += MemoryUsage.sizeOfInstance(ApproxMaxKCut.class);
        // Candidate solutions.
        memUsage += 2 * HugeByteArray.memoryEstimation(nodeCount);
        // Improvement costs cache.
        memUsage += HugeAtomicDoubleArray.memoryEstimation(nodeCount * k);
        // Set swap status cache.
        memUsage += HugeAtomicByteArray.memoryEstimation(nodeCount);

        if (vns) {
            // VNS neighbor candidate solution.
            memUsage += HugeByteArray.memoryEstimation(nodeCount);
        }

        return memUsage;
    }

    static Stream<Arguments> configParamsForMemoryEstimationTest() {
        return TestSupport.crossArguments(
            // node count
            () -> Stream.of(Arguments.of(10_000L), Arguments.of(40_000L)),
            // k
            () -> Stream.of(Arguments.of((byte) 2), Arguments.of((byte) 5)),
            // Using relationship weight
            () -> Stream.of(Arguments.of(true), Arguments.of(false)),
            // VNS max neighborhood order (0 means VNS not used)
            () -> Stream.of(Arguments.of(0), Arguments.of(4)),
            // concurrency
            () -> Stream.of(Arguments.of(1), Arguments.of(4))
        );
    }

    @ParameterizedTest
    @MethodSource("configParamsForMemoryEstimationTest")
    void memoryEstimation(long nodeCount, byte k, boolean weighted, int vnsMaxNeighborhoodOrder, int concurrency) {
        var configBuilder = ImmutableApproxMaxKCutConfig.builder();

        configBuilder.k(k);
        if (weighted) {
            configBuilder.relationshipWeightProperty("weight");
        }
        configBuilder.vnsMaxNeighborhoodOrder(vnsMaxNeighborhoodOrder);
        configBuilder.concurrency(concurrency);

        var config = configBuilder.build();

        var expectedMemory = memUsageHelper(nodeCount, k, vnsMaxNeighborhoodOrder > 0);

        assertMemoryEstimation(
            () -> new ApproxMaxKCutFactory<>().memoryEstimation(config),
            nodeCount,
            concurrency,
            expectedMemory,
            expectedMemory
        );
    }
}
