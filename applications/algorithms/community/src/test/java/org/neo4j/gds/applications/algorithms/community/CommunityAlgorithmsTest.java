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
import org.assertj.core.api.AssertionsForClassTypes;
import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfigImpl;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.LoggerForProgressTracking;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.hdbscan.HDBScanStreamConfig;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.scc.SccStreamConfigImpl;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfigImpl;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.wcc.WccStreamConfigImpl;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.compat.TestLog.INFO;

final class CommunityAlgorithmsTest {

    private CommunityAlgorithmsTest() {}






    @Nested
    @GdlExtension
    class Scc {

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
        void shouldLogProgress() {
            var config = SccStreamConfigImpl.builder().build();
            var log = new GdsTestLog();

            var progressTrackerCreator = progressTrackerCreator(1, log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.scc(graph, config);
            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "SCC :: Start",
                    "SCC 11%",
                    "SCC 22%",
                    "SCC 33%",
                    "SCC 44%",
                    "SCC 55%",
                    "SCC 66%",
                    "SCC 77%",
                    "SCC 88%",
                    "SCC 100%",
                    "SCC :: Finished"
                );
        }
    }

    @Nested
    class Wcc {

        private static final int SETS_COUNT = 16;
        private static final int SET_SIZE = 10;

        @ParameterizedTest
        @EnumSource(Orientation.class)
        void shouldLogProgress(Orientation orientation) {
            var graph = createTestGraph(orientation);

            var config = WccStreamConfigImpl.builder().concurrency(2).build();
            var log = new GdsTestLog();

            var progressTrackerCreator = progressTrackerCreator(2, log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.wcc(graph, config);

            var messagesInOrder = log.getMessages(INFO);

            AssertionsForInterfaceTypes.assertThat(messagesInOrder)
                // avoid asserting on the thread id
                .extracting(removingThreadId())
                .hasSize(103)
                .containsSequence(
                    "WCC :: Start",
                    "WCC 0%",
                    "WCC 1%",
                    "WCC 2%"
                )
                .containsSequence(
                    "WCC 98%",
                    "WCC 99%",
                    "WCC 100%",
                    "WCC :: Finished"
                );
        }

        private static Graph createTestGraph(Orientation orientation) {
            int[] setSizes = new int[SETS_COUNT];
            Arrays.fill(setSizes, SET_SIZE);

            StringBuilder gdl = new StringBuilder();

            for (int setSize : setSizes) {
                gdl.append(createLine(setSize));
            }

            return fromGdl(gdl.toString(), orientation);
        }

        static String createLine(int setSize) {
            return IntStream.range(0, setSize)
                .mapToObj(i -> "()")
                .collect(Collectors.joining("-[:REL]->"));
        }


    }

    @Nested
    @GdlExtension
    class SpeakerListenerLPA {

        // using an offset of 0 as the tests were written against a specific random seed
        @GdlGraph(idOffset = 0)
        private static final String GDL =
            "(x), (a), (b), (c), (d), (e), (f), (g), (h), (i)" +
                ", (a)-->(b)" +
                ", (a)-->(c)" +
                ", (b)-->(e)" +
                ", (b)-->(d)" +
                ", (b)-->(c)" +
                ", (e)-->(f)" +
                ", (f)-->(g)" +
                ", (f)-->(h)" +
                ", (f)-->(i)" +
                ", (h)-->(i)" +
                ", (g)-->(i)";

        @Inject
        private TestGraph graph;

        @Test
        void shouldLogProgress() {

            var config = SpeakerListenerLPAConfigImpl.builder().concurrency(1).maxIterations(5).build();
            var log = new GdsTestLog();

            var progressTrackerCreator = progressTrackerCreator(1, log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.speakerListenerLPA(graph, config);
            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "SpeakerListenerLPA :: Start",
                    "SpeakerListenerLPA :: Compute iteration 1 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 1 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 1 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 1 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 1 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 1 of 5 :: Finished",
                    "SpeakerListenerLPA :: Compute iteration 2 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 2 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 2 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 2 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 2 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 2 of 5 :: Finished",
                    "SpeakerListenerLPA :: Compute iteration 3 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 3 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 3 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 3 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 3 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 3 of 5 :: Finished",
                    "SpeakerListenerLPA :: Compute iteration 4 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 4 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 4 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 4 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 4 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 4 of 5 :: Finished",
                    "SpeakerListenerLPA :: Compute iteration 5 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 5 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 5 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 5 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 5 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 5 of 5 :: Finished",
                    "SpeakerListenerLPA :: Finished"
                );
        }

    }

    @GdlExtension
    @Nested
    class ApproxMaxCut {
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

        @ParameterizedTest
        @ValueSource(ints = {0, 2})
        void progressLogging(int vnsMaxNeighborhoodOrder) {
            var config = ApproxMaxKCutBaseConfigImpl.builder()
                .vnsMaxNeighborhoodOrder(vnsMaxNeighborhoodOrder)
                .build();
            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(4, log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.approximateMaximumKCut(graph, config);

            AssertionsForClassTypes.assertThat(log.containsMessage(TestLog.INFO, ":: Start")).isTrue();
            AssertionsForClassTypes.assertThat(log.containsMessage(TestLog.INFO, ":: Finish")).isTrue();

            for (int i = 1; i <= config.iterations(); i++) {
                AssertionsForClassTypes.assertThat(log.containsMessage(
                    TestLog.INFO,
                    "place nodes randomly %s of %s :: Start".formatted(i, config.iterations())
                )).isTrue();
                AssertionsForClassTypes.assertThat(log.containsMessage(
                    TestLog.INFO,
                    "place nodes randomly %s of %s 100%%".formatted(i, config.iterations())
                )).isTrue();
                AssertionsForClassTypes.assertThat(log.containsMessage(
                    TestLog.INFO,
                    "place nodes randomly %s of %s :: Finished".formatted(i, config.iterations())
                )).isTrue();

                if (vnsMaxNeighborhoodOrder == 0) {
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: Start".formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: Finished".formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: improvement loop :: Start".formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: improvement loop :: Finished".formatted(i, config.iterations())
                    )).isTrue();

                    // May occur several times but we don't know.
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: improvement loop :: compute node to community weights 1 :: Start"
                            .formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: improvement loop :: compute node to community weights 1 100%%"
                            .formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: improvement loop :: compute node to community weights 1 :: Finished"
                            .formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: improvement loop :: swap for local improvements 1 :: Start".formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: improvement loop :: swap for local improvements 1 100%%".formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: improvement loop :: swap for local improvements 1 :: Finished".formatted(i, config.iterations())
                    )).isTrue();

                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: compute current solution cost :: Start".formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: compute current solution cost 100%%".formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "local search %s of %s :: compute current solution cost :: Finished".formatted(i, config.iterations())
                    )).isTrue();
                } else {
                    // We merely check that VNS is indeed run. The rest is very similar to the non-VNS case.
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "variable neighborhood search %s of %s :: Start".formatted(i, config.iterations())
                    )).isTrue();
                    AssertionsForClassTypes.assertThat(log.containsMessage(
                        TestLog.INFO,
                        "variable neighborhood search %s of %s :: Finished".formatted(i, config.iterations())
                    )).isTrue();
                }
            }
        }
    }

    @Nested
    @GdlExtension
    class HDBScan {

        @GdlGraph
        private static final String DATA =
            """
            CREATE
                (a:Node {point: [1.17755754d, 2.02742572d]}),
                (b:Node {point: [0.88489682d, 1.97328227d]}),
                (c:Node {point: [1.04192267d, 4.34997048d]}),
                (d:Node {point: [1.25764886d, 1.94667762d]}),
                (e:Node {point: [0.95464318d, 1.55300632d]}),
                (f:Node {point: [0.80617459d, 1.60491802d]}),
                (g:Node {point: [1.26227786d, 3.96066446d]}),
                (h:Node {point: [0.87569985d, 4.51938412d]}),
                (i:Node {point: [0.8028515d , 4.088106d  ]}),
                (j:Node {point: [0.82954022d, 4.63897487d]})
            """;

        @Inject
        private TestGraph graph;

        @Test
        void shouldLogProgress() {

            var config = HDBScanStreamConfig.of(CypherMapWrapper.create(Map.of(
                "leafSize", 1L,
                "samples", 2,
                "minClusterSize", 2L,
                "nodeProperty", "point",
                "concurrency", 1
            )));
            var log = new GdsTestLog();

            var progressTrackerCreator = progressTrackerCreator(1, log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            var labels = algorithms.hdbscan(graph, config);
            assertThat(labels).isNotNull();

            var messagesInOrder = log.getMessages(INFO);

            assertThat(messagesInOrder)
                // avoid asserting on the thread id
                .extracting(removingThreadId())
                .contains(
                    "HDBScan :: Start",
                    "HDBScan :: KD-Tree Construction :: Start",
                    "HDBScan :: KD-Tree Construction :: Finished",
                    "HDBScan :: Nearest Neighbors Search :: Start",
                    "HDBScan :: Nearest Neighbors Search :: Finished",
                    "HDBScan :: MST Computation :: Start",
                    "HDBScan :: MST Computation :: Finished",
                    "HDBScan :: Dendrogram Creation :: Finished",
                    "HDBScan :: Condensed Tree Creation  :: Start",
                    "HDBScan :: Condensed Tree Creation  100%",
                    "HDBScan :: Condensed Tree Creation  :: Finished",
                    "HDBScan :: Node Labelling :: Start",
                    "HDBScan :: Node Labelling :: Stability calculation :: Start",
                    "HDBScan :: Node Labelling :: Stability calculation :: Finished",
                    "HDBScan :: Node Labelling :: cluster selection :: Start",
                    "HDBScan :: Node Labelling :: cluster selection :: Finished",
                    "HDBScan :: Node Labelling :: labelling :: Start",
                    "HDBScan :: Node Labelling :: labelling :: Finished",
                    "HDBScan :: Node Labelling :: Finished",
                    "HDBScan :: Finished"
                );
        }

    }


    abstract static class TestProgressTrackerCreator extends ProgressTrackerCreator {

        public TestProgressTrackerCreator(LoggerForProgressTracking log, RequestScopedDependencies requestScopedDependencies) {
            super(log, requestScopedDependencies);
        }

        abstract AtomicReference<TestProgressTracker> progressTracker();

    }

    TestProgressTrackerCreator progressTrackerCreator(int concurrency, Log log) {

        AtomicReference<TestProgressTracker> progressTrackerAtomicReference = new AtomicReference<>();
        var progressTrackerCreator = mock(TestProgressTrackerCreator.class);
        when(progressTrackerCreator.createProgressTracker(
            any(Task.class),
            any(),
            any(),
            anyBoolean()
        )).then(
            i ->
            {
                var taskProgressTracker = new TestProgressTracker(
                    i.getArgument(0, Task.class),
                    new LoggerForProgressTrackingAdapter(log),
                    new Concurrency(concurrency),
                    EmptyTaskRegistryFactory.INSTANCE
                );
                progressTrackerAtomicReference.set(taskProgressTracker);
                return taskProgressTracker;
            }
        );
        when(progressTrackerCreator.progressTracker()).thenReturn(progressTrackerAtomicReference);

        return progressTrackerCreator;
    }

}
