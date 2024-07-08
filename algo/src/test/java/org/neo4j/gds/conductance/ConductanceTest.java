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
package org.neo4j.gds.conductance;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.LogAdapter;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
final class ConductanceTest {

    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "natural")
    private static final String TEST_GRAPH =
        "CREATE" +
        "  (a:Label1 { community: 0 })" +
        ", (b:Label1 { community: 0 })" +
        ", (c:Label1 { community: 0 })" +
        ", (d:Label1 { community: 1 })" +
        ", (e:Label1 { community: 1 })" +
        ", (f:Label1 { community: 1 })" +
        ", (g:Label1 { community: 1 })" +
        ", (h:Label1 { community: -1 })" +

        ", (a)-[:TYPE1 {weight: 81.0}]->(b)" +
        ", (a)-[:TYPE1 {weight: 7.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE1 {weight: 1.0}]->(g)" +
        ", (b)-[:TYPE1 {weight: 3.0}]->(h)" +
        ", (c)-[:TYPE1 {weight: 45.0}]->(b)" +
        ", (c)-[:TYPE1 {weight: 3.0}]->(e)" +
        ", (d)-[:TYPE1 {weight: 3.0}]->(c)" +
        ", (e)-[:TYPE1 {weight: 1.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 3.0}]->(a)" +
        ", (g)-[:TYPE1 {weight: 4.0}]->(c)" +
        ", (g)-[:TYPE1 {weight: 999.0}]->(g)" +
        ", (h)-[:TYPE1 {weight: 2.0}]->(a)";

    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "undirected")
    private static final String UNDIRECTED = TEST_GRAPH;

    @Inject
    private TestGraph naturalGraph;

    @Inject
    private TestGraph undirectedGraph;

    private static Stream<Arguments> correctResultsParameters() {
        return TestSupport.crossArguments(
            () -> Stream.of(
                Arguments.of(
                    false,
                    Orientation.NATURAL,
                    Map.of(0L, 5.0 / (5.0 + 2.0), 1L, 4.0 / (4.0 + 1.0))
                ),
                Arguments.of(
                    true,
                    Orientation.NATURAL,
                    Map.of(0L, 15.0 / (15.0 + 126.0), 1L, 11.0 / (11.0 + 999.0))
                ),
                Arguments.of(
                    false,
                    Orientation.UNDIRECTED,
                    Map.of(0L, 10.0 / (10.0 + 4.0), 1L, 8.0 / (8.0 + 2.0))
                ),
                Arguments.of(
                    true,
                    Orientation.UNDIRECTED,
                    Map.of(0L, 28.0 / (28.0 + 252.0), 1L, 23.0 / (23.0 + 1998.0))
                )
            ),
            () -> Stream.of(Arguments.of(1), Arguments.of(4))  // concurrency
        );
    }

    @ParameterizedTest
    @MethodSource("correctResultsParameters")
    void computeCorrectResults(
        boolean weighted,
        Orientation orientation,
        Map<Long, Double> expectedConductances,
        int concurrency
    ) {
        var minBatchSize = concurrency > 1 ? 1 : 10_000;
        var conductance = new Conductance(
            orientation == Orientation.NATURAL ? naturalGraph : undirectedGraph,
            new Concurrency(4),
            minBatchSize,
            weighted,
            "community",
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER
        );

        var result = conductance.compute();
        var computedConductances = result.communityConductances();

        assertThat(computedConductances.get(0L)).isCloseTo(expectedConductances.get(0L), Offset.offset(0.0001));
        assertThat(computedConductances.get(1L)).isCloseTo(expectedConductances.get(1L), Offset.offset(0.0001));

        assertThat(result.globalAverageConductance()).isCloseTo(
            (computedConductances.get(0) + computedConductances.get(1)) / 2,
            Offset.offset(0.0001)
        );
    }

    @Test
    void logProgress() {
        var parameters = new ConductanceParameters(new Concurrency(1), 10_000, false, "community");
        var factory = new ConductanceAlgorithmFactory<>();
        var progressTask = factory.progressTask(naturalGraph.nodeCount());
        var log = Neo4jProxy.testLog();
        var progressTracker = new TaskProgressTracker(
            progressTask,
            new LogAdapter(log),
            parameters.concurrency(),
            EmptyTaskRegistryFactory.INSTANCE
        );

        factory.build(naturalGraph, parameters, progressTracker).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "Conductance :: Start",
                "Conductance :: count relationships :: Start",
                "Conductance :: count relationships 100%",
                "Conductance :: count relationships :: Finished",
                "Conductance :: accumulate counts :: Start",
                "Conductance :: accumulate counts 100%",
                "Conductance :: accumulate counts :: Finished",
                "Conductance :: perform conductance computations :: Start",
                "Conductance :: perform conductance computations 100%",
                "Conductance :: perform conductance computations :: Finished",
                "Conductance :: Finished"
            );
    }
}
