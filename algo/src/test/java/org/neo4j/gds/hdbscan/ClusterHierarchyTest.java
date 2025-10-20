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
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.logging.GdsTestLog;

import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@ExtendWith(SoftAssertionsExtension.class)
class ClusterHierarchyTest {

    @Test
    void shouldWorkWithLineGraph(SoftAssertions assertions) {
        var edges = HugeObjectArray.of(
            new Edge(1, 2, 3.),
            new Edge(0, 1, 5.)
        );

        var clusterHierarchy = ClusterHierarchy.create(
            3,
            edges,
            ProgressTracker.NULL_TRACKER
        );

        // 1. `1` and `2` are joined and create new id = 3 --> first set
        // 2. `0` and `3` are joined and create new id = 4 --> second set

        assertions.assertThat(clusterHierarchy.root()).isEqualTo(4L);

        assertions.assertThat(clusterHierarchy.left(4)).isEqualTo(0);
        assertions.assertThat(clusterHierarchy.right(4)).isEqualTo(3);
        assertions.assertThat(clusterHierarchy.lambda(4)).isEqualTo(5.);
        assertions.assertThat(clusterHierarchy.size(4)).isEqualTo(3);

        assertions.assertThat(clusterHierarchy.left(3)).isEqualTo(1);
        assertions.assertThat(clusterHierarchy.right(3)).isEqualTo(2);
        assertions.assertThat(clusterHierarchy.lambda(3)).isEqualTo(3.);
        assertions.assertThat(clusterHierarchy.size(3)).isEqualTo(2);

        assertions.assertThat(clusterHierarchy.size(0)).isEqualTo(1);
    }

    @Test
    void shouldWorkOnMoreComplexTree(SoftAssertions assertions) {
        var edges = HugeObjectArray.of(
            new Edge(2, 0, 0.24862277),
            new Edge(4, 0, 0.24862277),
            new Edge(3, 8, 0.28702033),
            new Edge(10, 8, 0.28702033),
            new Edge(5, 8, 0.31202177),
            new Edge(6, 2, 0.412653),
            new Edge(1, 3, 0.51812741),
            new Edge(9, 2, 0.55225731),
            new Edge(7, 6, 0.65362267),
            new Edge(5, 7, 1.42823558)
        );

        var clusterHierarchy = ClusterHierarchy.create(
            edges.size() + 1,
            edges,
            ProgressTracker.NULL_TRACKER
        );


        assertions.assertThat(clusterHierarchy.left(11)).isEqualTo(2);
        assertions.assertThat(clusterHierarchy.right(11)).isEqualTo(0);
        assertions.assertThat(clusterHierarchy.lambda(11)).isCloseTo(0.24862277, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(11)).isEqualTo(2);

        assertions.assertThat(clusterHierarchy.left(12)).isEqualTo(4);
        assertions.assertThat(clusterHierarchy.right(12)).isEqualTo(11);
        assertions.assertThat(clusterHierarchy.lambda(12)).isCloseTo(0.24862277, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(12)).isEqualTo(3);

        assertions.assertThat(clusterHierarchy.left(13)).isEqualTo(3);
        assertions.assertThat(clusterHierarchy.right(13)).isEqualTo(8);
        assertions.assertThat(clusterHierarchy.lambda(13)).isCloseTo(0.28702033, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(13)).isEqualTo(2);

        assertions.assertThat(clusterHierarchy.left(14)).isEqualTo(10);
        assertions.assertThat(clusterHierarchy.right(14)).isEqualTo(13);
        assertions.assertThat(clusterHierarchy.lambda(14)).isCloseTo(0.28702033, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(14)).isEqualTo(3);

        assertions.assertThat(clusterHierarchy.left(15)).isEqualTo(5);
        assertions.assertThat(clusterHierarchy.right(15)).isEqualTo(14);
        assertions.assertThat(clusterHierarchy.lambda(15)).isCloseTo(0.31202177, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(15)).isEqualTo(4);

        assertions.assertThat(clusterHierarchy.left(16)).isEqualTo(6);
        assertions.assertThat(clusterHierarchy.right(16)).isEqualTo(12);
        assertions.assertThat(clusterHierarchy.lambda(16)).isCloseTo(0.412653, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(16)).isEqualTo(4);

        assertions.assertThat(clusterHierarchy.left(17)).isEqualTo(1);
        assertions.assertThat(clusterHierarchy.right(17)).isEqualTo(15);
        assertions.assertThat(clusterHierarchy.lambda(17)).isCloseTo(0.51812741, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(17)).isEqualTo(5);

        assertions.assertThat(clusterHierarchy.left(18)).isEqualTo(9);
        assertions.assertThat(clusterHierarchy.right(18)).isEqualTo(16);
        assertions.assertThat(clusterHierarchy.lambda(18)).isCloseTo(0.55225731, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(18)).isEqualTo(5);

        assertions.assertThat(clusterHierarchy.left(19)).isEqualTo(7);
        assertions.assertThat(clusterHierarchy.right(19)).isEqualTo(18);
        assertions.assertThat(clusterHierarchy.lambda(19)).isCloseTo(0.65362267, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(19)).isEqualTo(6);

        assertions.assertThat(clusterHierarchy.left(20)).isEqualTo(17);
        assertions.assertThat(clusterHierarchy.right(20)).isEqualTo(19);
        assertions.assertThat(clusterHierarchy.lambda(20)).isCloseTo(1.42823558, Offset.offset(1e-9));
        assertions.assertThat(clusterHierarchy.size(20)).isEqualTo(11);
    }

    @Test
    void shouldLogProgress(){
        var edges = HugeObjectArray.of(
            new Edge(1, 2, 3.),
            new Edge(0, 1, 5.)
        );

        var progressTask = HDBScanProgressTrackerCreator.hierarchyTask("foo",3);
        var log = new GdsTestLog();
        var progressTracker = TaskProgressTracker.create(progressTask, new LoggerForProgressTrackingAdapter(log), new Concurrency(1), EmptyTaskRegistryFactory.INSTANCE);

        ClusterHierarchy.create(3, edges, progressTracker);

        Assertions.assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "foo :: Start",
                "foo 50%",
                "foo 100%",
                "foo :: Finished"
            );
    }
}
