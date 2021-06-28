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
package org.neo4j.graphalgo.impl.approxmaxkcut;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void shouldComputeCorrectResults(
        boolean weighted,
        Map<String, Long> expectedMapping,
        double expectedCost,
        int vnsMaxNeighborhoodOrder,
        int concurrency
    ) {
        var configBuilder = ImmutableApproxMaxKCutConfig.builder()
            .concurrency(concurrency)
            .k(2)
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
            ProgressLogger.NULL_LOGGER,
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

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void testProgressLogging(int vnsMaxNeighborhoodOrder) {
        var configBuilder = ImmutableApproxMaxKCutConfig.builder();
        configBuilder.vnsMaxNeighborhoodOrder(vnsMaxNeighborhoodOrder);

        var config = configBuilder.build();

        TestProgressLogger progressLogger = new TestProgressLogger(graph.nodeCount(), "Approximate Maximum k-cut", 1);
        var approxMaxKCut = new ApproxMaxKCut(
            graph,
            Pools.DEFAULT,
            config,
            progressLogger,
            AllocationTracker.empty()
        );

        approxMaxKCut.compute();

        List<AtomicLong> progresses = progressLogger.getProgresses();

        assertEquals(1, progresses.size());

        assertTrue(progressLogger.containsMessage(TestLog.INFO, ":: Start"));
        assertTrue(progressLogger.containsMessage(TestLog.INFO, ":: Finish"));

        for (int i = 1; i <= config.iterations(); i++) {
            assertTrue(progressLogger.containsMessage(
                TestLog.INFO,
                String.format("Iteration %d: Randomly assign nodes to communities :: Start", i)
            ));
            assertTrue(progressLogger.containsMessage(
                TestLog.INFO,
                String.format("Iteration %d: Randomly assign nodes to communities :: Finished", i)
            ));

            if (vnsMaxNeighborhoodOrder > 0) {
                assertTrue(progressLogger.containsMessage(
                    TestLog.INFO,
                    String.format("Iteration %d: Variable neighborhood search :: Start", i)
                ));
                assertTrue(progressLogger.containsMessage(
                    TestLog.INFO,
                    String.format("Iteration %d: Variable neighborhood search :: Finished", i)
                ));
            } else {
                assertTrue(progressLogger.containsMessage(
                    TestLog.INFO,
                    String.format("Iteration %d: Local search :: Start", i)
                ));
                assertTrue(progressLogger.containsMessage(
                    TestLog.INFO,
                    String.format("Iteration %d: Local search :: Finished", i)
                ));
            }
        }
    }
}
