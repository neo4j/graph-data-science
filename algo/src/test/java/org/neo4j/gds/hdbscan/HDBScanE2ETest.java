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
package org.neo4j.gds.hdbscan;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.termination.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class HDBScanE2ETest {

    @Nested
    class GraphWithDoubleProperties {
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
        void hdbscan() {
            var hdbScan = new HDBScan(
                graph,
                graph.nodeProperties("point"),
                new Concurrency(1),
                1,
                2,
                2,
                ProgressTracker.NULL_TRACKER,
                TerminationFlag.RUNNING_TRUE
            );

            var result = hdbScan.compute();

            var labelsWithOffset = result.labels();

            var labels = new long[10];
            for (char letter = 'a'; letter <= 'j'; ++letter) {
                var offsetPosition = graph.toMappedNodeId(String.valueOf(letter));
                labels[letter - 'a'] = labelsWithOffset.get(offsetPosition);
            }

            var expectedLabels = new long[]{2, 2, 1, 2, 2, 2, 1, 1, 1, 1};

            assertThat(result.numberOfClusters()).isEqualTo(2L);
            assertThat(result.numberOfNoisePoints()).isEqualTo(0);

            assertThat(labels).containsExactly(expectedLabels);
        }

        @Test
        void shouldLogProgress() {

            var progressTask = HDBScanProgressTrackerCreator.hdbscanTask("foo", graph.nodeCount());
            var log = new GdsTestLog();
            var progressTracker = TaskProgressTracker.create(
                progressTask,
                new LoggerForProgressTrackingAdapter(log),
                new Concurrency(1),
                PlainSimpleRequestCorrelationId.create(),
                EmptyTaskRegistryFactory.INSTANCE
            );

            new HDBScan(
                graph,
                graph.nodeProperties("point"),
                new Concurrency(1),
                1,
                2,
                2,
                progressTracker,
                TerminationFlag.RUNNING_TRUE
            ).compute();

            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "foo :: Start",
                    "foo :: KD-Tree Construction :: Start",
                    "foo :: KD-Tree Construction 10%",
                    "foo :: KD-Tree Construction 20%",
                    "foo :: KD-Tree Construction 30%",
                    "foo :: KD-Tree Construction 40%",
                    "foo :: KD-Tree Construction 50%",
                    "foo :: KD-Tree Construction 60%",
                    "foo :: KD-Tree Construction 70%",
                    "foo :: KD-Tree Construction 80%",
                    "foo :: KD-Tree Construction 90%",
                    "foo :: KD-Tree Construction 100%",
                    "foo :: KD-Tree Construction :: Finished",
                    "foo :: Nearest Neighbors Search :: Start",
                    "foo :: Nearest Neighbors Search 10%",
                    "foo :: Nearest Neighbors Search 20%",
                    "foo :: Nearest Neighbors Search 30%",
                    "foo :: Nearest Neighbors Search 40%",
                    "foo :: Nearest Neighbors Search 50%",
                    "foo :: Nearest Neighbors Search 60%",
                    "foo :: Nearest Neighbors Search 70%",
                    "foo :: Nearest Neighbors Search 80%",
                    "foo :: Nearest Neighbors Search 90%",
                    "foo :: Nearest Neighbors Search 100%",
                    "foo :: Nearest Neighbors Search :: Finished",
                    "foo :: MST Computation :: Start",
                    "foo :: MST Computation 11%",
                    "foo :: MST Computation 22%",
                    "foo :: MST Computation 33%",
                    "foo :: MST Computation 44%",
                    "foo :: MST Computation 55%",
                    "foo :: MST Computation 66%",
                    "foo :: MST Computation 77%",
                    "foo :: MST Computation 88%",
                    "foo :: MST Computation 100%",
                    "foo :: MST Computation :: Finished",
                    "foo :: Dendrogram Creation :: Start",
                    "foo :: Dendrogram Creation 11%",
                    "foo :: Dendrogram Creation 22%",
                    "foo :: Dendrogram Creation 33%",
                    "foo :: Dendrogram Creation 44%",
                    "foo :: Dendrogram Creation 55%",
                    "foo :: Dendrogram Creation 66%",
                    "foo :: Dendrogram Creation 77%",
                    "foo :: Dendrogram Creation 88%",
                    "foo :: Dendrogram Creation 100%",
                    "foo :: Dendrogram Creation :: Finished",
                    "foo :: Condensed Tree Creation  :: Start",
                    "foo :: Condensed Tree Creation  11%",
                    "foo :: Condensed Tree Creation  22%",
                    "foo :: Condensed Tree Creation  33%",
                    "foo :: Condensed Tree Creation  44%",
                    "foo :: Condensed Tree Creation  55%",
                    "foo :: Condensed Tree Creation  66%",
                    "foo :: Condensed Tree Creation  77%",
                    "foo :: Condensed Tree Creation  88%",
                    "foo :: Condensed Tree Creation  100%",
                    "foo :: Condensed Tree Creation  :: Finished",
                    "foo :: Node Labelling :: Start",
                    "foo :: Node Labelling :: Stability calculation :: Start",
                    "foo :: Node Labelling :: Stability calculation 11%",
                    "foo :: Node Labelling :: Stability calculation 22%",
                    "foo :: Node Labelling :: Stability calculation 100%",
                    "foo :: Node Labelling :: Stability calculation :: Finished",
                    "foo :: Node Labelling :: cluster selection :: Start",
                    "foo :: Node Labelling :: cluster selection 11%",
                    "foo :: Node Labelling :: cluster selection 22%",
                    "foo :: Node Labelling :: cluster selection 33%",
                    "foo :: Node Labelling :: cluster selection 100%",
                    "foo :: Node Labelling :: cluster selection :: Finished",
                    "foo :: Node Labelling :: labelling :: Start",
                    "foo :: Node Labelling :: labelling 5%",
                    "foo :: Node Labelling :: labelling 10%",
                    "foo :: Node Labelling :: labelling 15%",
                    "foo :: Node Labelling :: labelling 21%",
                    "foo :: Node Labelling :: labelling 26%",
                    "foo :: Node Labelling :: labelling 31%",
                    "foo :: Node Labelling :: labelling 36%",
                    "foo :: Node Labelling :: labelling 42%",
                    "foo :: Node Labelling :: labelling 47%",
                    "foo :: Node Labelling :: labelling 52%",
                    "foo :: Node Labelling :: labelling 57%",
                    "foo :: Node Labelling :: labelling 63%",
                    "foo :: Node Labelling :: labelling 68%",
                    "foo :: Node Labelling :: labelling 100%",
                    "foo :: Node Labelling :: labelling :: Finished",
                    "foo :: Node Labelling :: Finished",
                    "foo :: Finished"
                );

        }

    }

    @Nested
    class GraphWithFloatProperties {
        @GdlGraph
        private static final String DATA =
            """
                CREATE
                    (a:Node {point: [1.17755754f, 2.02742572f]}),
                    (b:Node {point: [0.88489682f, 1.97328227f]}),
                    (c:Node {point: [1.04192267f, 4.34997048f]}),
                    (d:Node {point: [1.25764886f, 1.94667762f]}),
                    (e:Node {point: [0.95464318f, 1.55300632f]}),
                    (f:Node {point: [0.80617459f, 1.60491802f]}),
                    (g:Node {point: [1.26227786f, 3.96066446f]}),
                    (h:Node {point: [0.87569985f, 4.51938412f]}),
                    (i:Node {point: [0.8028515f , 4.088106f  ]}),
                    (j:Node {point: [0.82954022f, 4.63897487f]})
                """;

        @Inject
        private TestGraph graph;

        @Test
        void hdbscan() {
            var hdbScan = new HDBScan(
                graph,
                graph.nodeProperties("point"),
                new Concurrency(1),
                1,
                2,
                2,
                ProgressTracker.NULL_TRACKER,
                TerminationFlag.RUNNING_TRUE
            );

            var result = hdbScan.compute();

            var labelsWithOffset = result.labels();

            var labels = new long[10];
            for (char letter = 'a'; letter <= 'j'; ++letter) {
                var offsetPosition = graph.toMappedNodeId(String.valueOf(letter));
                labels[letter - 'a'] = labelsWithOffset.get(offsetPosition);
            }

            var expectedLabels = new long[]{2, 2, 1, 2, 2, 2, 1, 1, 1, 1};

            assertThat(result.numberOfClusters()).isEqualTo(2L);
            assertThat(result.numberOfNoisePoints()).isEqualTo(0);

            assertThat(labels).containsExactly(expectedLabels);
        }

    }

}
