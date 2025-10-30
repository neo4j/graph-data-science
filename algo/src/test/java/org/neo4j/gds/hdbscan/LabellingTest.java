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

import com.carrotsearch.hppc.BitSet;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.logging.GdsTestLog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

class LabellingTest {

    @Test
    void clusterStability() {
        var nodeCount = 4;
        var root = 4;

        var parent = HugeLongArray.of(5, 5, 6, 6, 0, 4, 4);
        var lambda = HugeDoubleArray.of(10, 10, 11, 11, 0, 12, 12);
        var size = HugeLongArray.of(4, 2, 2);
        var maximumClusterId = 6;

        var condensedTree = new CondensedTree(root, parent, lambda, size, maximumClusterId, nodeCount);
        var stabilityStep = new LabellingStep(condensedTree, nodeCount, ProgressTracker.NULL_TRACKER);

        var stabilities = stabilityStep.computeStabilities();


        assertThat(stabilities.toArray()).containsExactly(
            // stability of 4
            (1 / 12. - 0) + (1 / 12. - 0) + (1 / 12. - 0) + (1 / 12. - 0),
            // stability of 5
            (1 / 10. - 1 / 12.) + (1 / 10. - 1 / 12.),
            // stability of 6
            (1 / 11. - 1 / 12.) + (1 / 11. - 1 / 12.)
        );
    }

    @Test
    void clusterStabilityBiggerTest() {
        var parent = HugeLongArray.of(8, 8, 10, 10, 11, 11, 11, 0, 7, 7, 9, 9, 0);
        var lambda = HugeDoubleArray.of(11.0, 11.0, 9.0, 9.0, 8.0, 7.0, 7.0, 0.0, 12.0, 12.0, 10.0, 10.0, 0.0);
        var size = HugeLongArray.of(7, 2, 5, 2, 3, 0, 0);
        var maximumClusterId = 11;
        var nodeCount = 7;
        var root = 7;

        var condensedTree = new CondensedTree(root, parent, lambda, size, maximumClusterId, nodeCount);

        var stabilityStep = new LabellingStep(condensedTree, nodeCount,ProgressTracker.NULL_TRACKER);

        var stabilities = stabilityStep.computeStabilities();

        assertThat(stabilities.toArray()).containsExactly(
            new double[] {
                // stability of 7
                7 * 1. / 12,
                // stability of 8
                2 * (1. / 11 - 1. / 12),
                // stability of 9
                5 * (1. / 10 - 1. / 12),
                // stability of 10
                2 * (1. / 9 - 1. / 10),
                // stability of 11
                (1. / 8 - 1. / 10) + 2 * (1. / 7 - 1. / 10),
                0.0
            }, Offset.offset(1e-10));
    }

    @Test
    void clusterSelectionOfChildClusters() {
        //    3
        //   4  5

        var nodeCount = 3;
        var root = 3;

        var parent = HugeLongArray.of(-1, -1, -1, 0, 3, 3);
        var lambda = HugeDoubleArray.of(-1, -1, -1, -1, -1, -1);
        var size = HugeLongArray.of(-1, -1, -1);
        var maximumClusterId = 5;

        var stabilities = HugeDoubleArray.of(3., 4., 5.);

        var condensedTree = new CondensedTree(root, parent, lambda, size, maximumClusterId, nodeCount);
        var stabilityStep = new LabellingStep(condensedTree, nodeCount,ProgressTracker.NULL_TRACKER);

        var selectedClusters = stabilityStep.selectedClusters(stabilities);

        assertThat(selectedClusters.get(0))
            .withFailMessage("Root should be unselected")
            .isFalse();
        assertThat(selectedClusters.get(1))
            .withFailMessage("First child should be selected cluster")
            .isTrue();
        assertThat(selectedClusters.get(2))
            .withFailMessage("Second child should be selected cluster")
            .isTrue();
    }

    @Test
    void clusterSelectionOfParentCluster() {
        //    3
        //   4  5

        var nodeCount = 3;
        var root = 3;

        var parent = HugeLongArray.of(-1, -1, -1, 0, 3, 3);
        var lambda = HugeDoubleArray.of(-1, -1, -1, -1, -1, -1);
        var size = HugeLongArray.of(-1, -1, -1);
        var maximumClusterId = 5;

        var stabilities = HugeDoubleArray.of(10., 4., 5.);

        var condensedTree = new CondensedTree(root, parent, lambda, size, maximumClusterId, nodeCount);
        var stabilityStep = new LabellingStep(condensedTree, nodeCount,ProgressTracker.NULL_TRACKER);

        var selectedClusters = stabilityStep.selectedClusters(stabilities);

        assertThat(selectedClusters.get(0))
            .withFailMessage("Root should be selected")
            .isTrue();
        assertThat(selectedClusters.get(1))
            .withFailMessage("First child should be selected")
            .isTrue();
        assertThat(selectedClusters.get(2))
            .withFailMessage("Second child should be selected")
            .isTrue();
    }

    @Test
    void labelling() {
        var parent = HugeLongArray.of(8, 8, 10, 10, 11, 11, 11, 0, 7, 7, 9, 9, 0);
        var lambda = HugeDoubleArray.of(11.0, 11.0, 9.0, 9.0, 8.0, 7.0, 7.0, 0.0, 12.0, 12.0, 10.0, 10.0, 0.0);
        var size = HugeLongArray.of(7, 2, 5, 2, 3, 0, 0);
        var maximumClusterId = 11;
        var nodeCount = 7;
        var root = 7;

        var condensedTree = new CondensedTree(root, parent, lambda, size, maximumClusterId, nodeCount);
        var selectedClusters = new BitSet(5);
        // selects cluster `8`
        selectedClusters.set(1);
        // selects cluster `11`
        selectedClusters.set(4);

        var stabilityStep = new LabellingStep(condensedTree, nodeCount,ProgressTracker.NULL_TRACKER);

        var labelsResult = stabilityStep.computeLabels(selectedClusters);

        var labels=labelsResult.labels();
        assertThat(labels.size()).isEqualTo(nodeCount);

        assertThat(labels.get(0)).isEqualTo(1L);
        assertThat(labels.get(1)).isEqualTo(1L);

        assertThat(labels.get(2)).isEqualTo(-1L);
        assertThat(labels.get(3)).isEqualTo(-1L);

        assertThat(labels.get(4)).isEqualTo(4L);
        assertThat(labels.get(5)).isEqualTo(4L);
        assertThat(labels.get(6)).isEqualTo(4L);

        assertThat(labelsResult.numberOfNoisePoints()).isEqualTo(2L);
        assertThat(labelsResult.numberOfClusters()).isEqualTo(2L);


    }

    @Test
    void labellingWhenAllClustersAreSelected() {
        var parent = HugeLongArray.of(8, 8, 10, 10, 11, 11, 11, 0, 7, 7, 9, 9, 0);
        var lambda = HugeDoubleArray.of(11.0, 11.0, 9.0, 9.0, 8.0, 7.0, 7.0, 0.0, 12.0, 12.0, 10.0, 10.0, 0.0);
        var size = HugeLongArray.of(7, 2, 5, 2, 3, 0, 0);
        var maximumClusterId = 11;
        var nodeCount = 7;
        var root = 7;

        var condensedTree = new CondensedTree(root, parent, lambda, size, maximumClusterId, nodeCount);
        var selectedClusters = new BitSet(5);
        selectedClusters.set(0, 5);

        var stabilityStep = new LabellingStep(condensedTree, nodeCount,ProgressTracker.NULL_TRACKER);

        var labelsResult = stabilityStep.computeLabels(selectedClusters);

        var labels= labelsResult.labels();
        assertThat(labels.size()).isEqualTo(nodeCount);
        assertThat(labels.toArray()).containsOnly(0L);

        assertThat(labelsResult.numberOfClusters()).isEqualTo(1L);
        assertThat(labelsResult.numberOfNoisePoints()).isEqualTo(0L);

    }

    @Test
    void shouldLogProgress(){
        var nodeCount = 4;
        var root = 4;

        var parent = HugeLongArray.of(5, 5, 6, 6, 0, 4, 4);
        var lambda = HugeDoubleArray.of(10, 10, 11, 11, 0, 12, 12);
        var size = HugeLongArray.of(4, 2, 2);
        var maximumClusterId = 6;

        var condensedTree = new CondensedTree(root, parent, lambda, size, maximumClusterId, nodeCount);

        var progressTask = HDBScanProgressTrackerCreator.labellingTask("foo",nodeCount);
        var log = new GdsTestLog();
        var progressTracker = TaskProgressTracker.create(
            progressTask,
            new LoggerForProgressTrackingAdapter(log),
            new Concurrency(1),
            PlainSimpleRequestCorrelationId.create(),
            EmptyTaskRegistryFactory.INSTANCE
        );

        new LabellingStep(condensedTree, nodeCount, progressTracker).labels();

        Assertions.assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "foo :: Start",
                "foo :: Stability calculation :: Start",
                "foo :: Stability calculation 33%",
                "foo :: Stability calculation 66%",
                "foo :: Stability calculation 100%",
                "foo :: Stability calculation :: Finished",
                "foo :: cluster selection :: Start",
                "foo :: cluster selection 33%",
                "foo :: cluster selection 66%",
                "foo :: cluster selection 100%",
                "foo :: cluster selection :: Finished",
                "foo :: labelling :: Start",
                "foo :: labelling 14%",
                "foo :: labelling 28%",
                "foo :: labelling 42%",
                "foo :: labelling 57%",
                "foo :: labelling 71%",
                "foo :: labelling 85%",
                "foo :: labelling 100%",
                "foo :: labelling :: Finished",
                "foo :: Finished"
            );
    }
}
