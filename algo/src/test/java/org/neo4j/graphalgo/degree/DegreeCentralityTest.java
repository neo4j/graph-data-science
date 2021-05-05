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
package org.neo4j.graphalgo.degree;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertMemoryEstimation;

@GdlExtension
final class DegreeCentralityTest {

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
        ", (h:Label1)" +
        ", (i:Label1)" +
        ", (j:Label1)" +

        ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +

        ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (d)-[:TYPE1 {weight: 2.0}]->(a)" +
        ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(d)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(f)" +

        ", (f)-[:TYPE1 {weight: 4.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: -2.0}]->(e)";

    @Inject
    private TestGraph graph;

    static Stream<Arguments> degreeCentralityParameters() {
        return TestSupport.crossArguments(
            () -> Stream.of(
                // Orientation NATURAL
                Arguments.of(
                    true,
                    Orientation.NATURAL,
                    Map.of("a", 0.0D, "b", 2.0D, "c", 2.0D, "d", 4.0D, "e", 6.0D, "f", 4.0D)
                ),
                Arguments.of(
                    true,
                    Orientation.NATURAL,
                    Map.of("a", 0.0D, "b", 2.0D, "c", 2.0D, "d", 4.0D, "e", 6.0D, "f", 4.0D)
                ),
                Arguments.of(
                    false,
                    Orientation.NATURAL,
                    Map.of("a", 0.0D, "b", 1.0D, "c", 1.0D, "d", 2.0D, "e", 3.0D, "f", 2.0D)
                ),
                // Orientation REVERSE
                Arguments.of(
                    false,
                    Orientation.REVERSE,
                    Map.of("a", 0.0D, "b", 4.0D, "c", 1.0D, "d", 1.0D, "e", 1.0D, "f", 1.0D)
                )
            ),
            () -> Stream.of(Arguments.of(1), Arguments.of(4)));
    }

    @ParameterizedTest
    @MethodSource("degreeCentralityParameters")
    void shouldComputeCorrectResults(boolean weighted, Orientation orientation, Map<String, Double> expected, int concurrency) {
        var configBuilder = ImmutableDegreeCentralityConfig.builder()
            .concurrency(concurrency)
            .orientation(orientation);

        if (weighted) {
            configBuilder.relationshipWeightProperty("weight");
        }

        var config = configBuilder.build();

        var degreeCentrality = new DegreeCentrality(
            graph,
            Pools.DEFAULT,
            config,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );

        var degreeFunction = degreeCentrality.compute();
        expected.forEach((variable, expectedDegree) -> {
            long nodeId = graph.toMappedNodeId(variable);
            assertEquals(expectedDegree, degreeFunction.get(nodeId), 1E-6);
        });
    }

    static Stream<Arguments> configParamsAndExpectedMemory() {
        return Stream.of(
            Arguments.of(true, 1, MemoryUsage.sizeOfInstance(DegreeCentrality.class) + HugeDoubleArray.memoryEstimation(10_000L)),
            Arguments.of(true, 4, MemoryUsage.sizeOfInstance(DegreeCentrality.class) + HugeDoubleArray.memoryEstimation(10_000L)),
            Arguments.of(false, 1, MemoryUsage.sizeOfInstance(DegreeCentrality.class)),
            Arguments.of(false, 4, MemoryUsage.sizeOfInstance(DegreeCentrality.class))
        );
    }

    @ParameterizedTest
    @MethodSource("configParamsAndExpectedMemory")
    void testMemoryEstimation(boolean weighted, int concurrency, long expectedMemory) {
        var configBuilder = ImmutableDegreeCentralityConfig.builder();
        if (weighted) {
            configBuilder.relationshipWeightProperty("weight");
        }
        configBuilder.concurrency(concurrency);
        var config = configBuilder.build();
        assertMemoryEstimation(
            () -> new DegreeCentralityFactory<>().memoryEstimation(config),
            10_000L,
            concurrency,
            expectedMemory,
            expectedMemory
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testProgressLogging(boolean weighted) {
        var configBuilder = ImmutableDegreeCentralityConfig.builder();
        if (weighted) {
            configBuilder.relationshipWeightProperty("weight");
        }
        var config = configBuilder.build();

        TestProgressLogger progressLogger = new TestProgressLogger(graph.nodeCount(), "Degree centrality", 1);
        var degreeCentrality = new DegreeCentrality(
            graph,
            Pools.DEFAULT,
            config,
            progressLogger,
            AllocationTracker.empty()
        );

        degreeCentrality.compute();
        List<AtomicLong> progresses = progressLogger.getProgresses();

        assertEquals(1, progresses.size());
        assertEquals(graph.nodeCount(), progresses.get(0).longValue());

        progressLogger.containsMessage(TestLog.INFO, ":: Start");
        progressLogger.containsMessage(TestLog.INFO, ":: Finish");
    }
}
