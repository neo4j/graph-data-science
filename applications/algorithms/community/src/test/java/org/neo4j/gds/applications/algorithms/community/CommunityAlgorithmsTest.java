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
package org.neo4j.gds.applications.algorithms.community;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.conductance.ConductanceStreamConfigImpl;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfigImpl;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfigImpl;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

final class CommunityAlgorithmsTest {

    private CommunityAlgorithmsTest() {}

    @GdlExtension
    @Nested
    class KCore {

        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (z:node)," +
                "  (a:node)," +
                "  (b:node)," +
                "  (c:node)," +
                "  (d:node)," +
                "  (e:node)," +
                "  (f:node)," +
                "  (g:node)," +
                "  (h:node)," +

                "(a)-[:R]->(b)," +
                "(b)-[:R]->(c)," +
                "(c)-[:R]->(d)," +
                "(d)-[:R]->(e)," +
                "(e)-[:R]->(f)," +
                "(f)-[:R]->(g)," +
                "(g)-[:R]->(h)," +
                "(h)-[:R]->(c)";


        @Inject
        private TestGraph graph;


        @Test
        void shouldLogProgressForKcore() {
            var config = KCoreDecompositionStreamConfigImpl.builder().build();
            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(4,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.kCore(graph, config);

            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "KCoreDecomposition :: Start",
                    "KCoreDecomposition 11%",
                    "KCoreDecomposition 33%",
                    "KCoreDecomposition 100%",
                    "KCoreDecomposition :: Finished"
                );
        }
    }

    @GdlExtension
    @Nested
    class Conductance{

        @GdlGraph(orientation = Orientation.NATURAL)
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

        @Inject
        private TestGraph graph;

        @Test
        void logProgress() {
            var config = ConductanceStreamConfigImpl.builder().concurrency(1).communityProperty("community").build();
            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(1,log);
            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.conductance(graph, config);

            Assertions.assertThat(log.getMessages(TestLog.INFO))
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

    @Nested
    class K1Coloring{

        @Test
        void shouldLogProgress(){
            var graph = RandomGraphGenerator.builder()
                .nodeCount(100)
                .averageDegree(10)
                .relationshipDistribution(RelationshipDistribution.UNIFORM)
                .seed(42L)
                .build()
                .generate();


            var config = K1ColoringStreamConfigImpl.builder()
                .concurrency(new Concurrency(4))
                .maxIterations(10)
                .build();

            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(1,log);
            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
           var result=algorithms.k1Coloring(graph,config);

            assertTrue(log.containsMessage(TestLog.INFO, ":: Start"));
            LongStream.range(1, result.ranIterations() + 1).forEach(iteration ->
                assertThat(log.getMessages(TestLog.INFO)).anyMatch(message -> {
                    var expected = "%d of %d".formatted(iteration, config.maxIterations());
                    return message.contains(expected);
                })
            );
            assertTrue(log.containsMessage(TestLog.INFO, ":: Finished"));
        }

    }

    static  ProgressTrackerCreator progressTrackerCreator(int concurrency, Log log) {

        var progressTrackerCreator = mock(ProgressTrackerCreator.class);
        when(progressTrackerCreator.createProgressTracker(any(), any(Task.class))).then(
            i ->
                new TaskProgressTracker(
                    i.getArgument(1),
                    log,
                    new Concurrency(concurrency),
                    EmptyTaskRegistryFactory.INSTANCE
                )
        );
        return progressTrackerCreator;
    }
}


