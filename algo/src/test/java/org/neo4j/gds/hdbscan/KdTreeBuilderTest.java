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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.TestLog;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class KdTreeBuilderTest {

    @GdlGraph
    private static final String DATA =
        """
                (a:Node { point: [2.0d, 3.0d]}),
                (b:Node { point: [5.0d, 4.0d]}),
                (c:Node { point: [9.0d, 6.0d]}),
                (d:Node { point: [4.0d, 7.0d]}),
                (e:Node { point: [8.0d, 1.0d]}),
                (f:Node { point: [7.0d, 2.0d]})
            """;

    @Inject
    private TestGraph graph;

    @Test
    void shouldCreateKdTree() {
        assertThat(graph.nodeCount())
            .isEqualTo(6);
        assertThat(graph.relationshipCount())
            .isZero();

        var points = graph.nodeProperties("point");

        var kdTree = new KdTreeBuilder(graph,
            points,
            1,
            1,
            new DoubleArrayDistances(points),
            ProgressTracker.NULL_TRACKER
        ).build();

        assertThat(kdTree).isNotNull();

        var root = kdTree.root();
        assertThat(root).isNotNull();
        assertThat(root.start()).isEqualTo(0);
        assertThat(root.end()).isEqualTo(graph.nodeCount());
        assertThat(kdTree.nodesContained(root)).containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L, 5L);
        var rootAABB = (DoubleAABB)root.aabb();
        assertThat(rootAABB.dimension()).isEqualTo(2);
        assertThat(rootAABB.min()).containsExactly(2, 1);
        assertThat(rootAABB.max()).containsExactly(9, 7);

        // LEFT
        // (a:Node[2.0, 3.0]}),
        // (d:Node { point: [4.0, 7.0]}),
        // (b:Node { point: [5.0, 4.0]}),
        var left = root.leftChild();


        assertThat(left.start()).isEqualTo(0);
        assertThat(left.end()).isEqualTo(3);
        assertThat(kdTree.nodesContained(left)).containsExactly(
            graph.toMappedNodeId("a"),
            graph.toMappedNodeId("b"),
            graph.toMappedNodeId("d")
        );

        var leftAABB = (DoubleAABB)left.aabb();

        assertThat(leftAABB.min()).containsExactly(2.0, 3.0);
        assertThat(leftAABB.max()).containsExactly(5.0, 7.0);

        var leftright = left.rightChild();
        var leftrightaabb =(DoubleAABB) leftright.aabb();
        assertThat(leftright.isLeaf()).isTrue();
        assertThat(kdTree.nodesContained(leftright)).containsExactly(graph.toMappedNodeId("d"));
        assertThat(leftrightaabb.min()).containsExactly(4.0, 7.0);
        assertThat(leftrightaabb.max()).containsExactly(4.0, 7.0);
        assertThat(leftright.start()).isEqualTo(2);
        assertThat(leftright.end()).isEqualTo(3);

        var leftleft = left.leftChild();
        var leftleftaabb = (DoubleAABB)leftleft.aabb();
        assertThat(leftleft.isLeaf()).isFalse();
        assertThat(kdTree.nodesContained(leftleft)).containsExactly(
            graph.toMappedNodeId("a"),
            graph.toMappedNodeId("b")
        );
        assertThat(leftleftaabb.min()).containsExactly(2.0, 3.0);
        assertThat(leftleftaabb.max()).containsExactly(5.0, 4.0);
        assertThat(leftleft.start()).isEqualTo(0);
        assertThat(leftleft.end()).isEqualTo(2);

        var leftleftleft = leftleft.leftChild();
        var  leftleftleftaabb= (DoubleAABB)leftleftleft.aabb();
        assertThat(leftleftleft.isLeaf()).isTrue();
        assertThat(kdTree.nodesContained(leftleftleft)).containsExactly(graph.toMappedNodeId("a"));
        assertThat(leftleftleftaabb.min()).containsExactly(2.0, 3.0);
        assertThat(leftleftleftaabb.max()).containsExactly(2.0, 3.0);
        assertThat(leftleftleft.start()).isEqualTo(0);
        assertThat(leftleftleft.end()).isEqualTo(1);

        var leftleftright = leftleft.rightChild();
        var  leftleftrightaabb = (DoubleAABB)leftleftright.aabb();
        assertThat(leftleftright.isLeaf()).isTrue();
        assertThat(kdTree.nodesContained(leftleftright)).containsExactly(graph.toMappedNodeId("b"));
        assertThat(leftleftrightaabb.min()).containsExactly(5.0, 4.0);
        assertThat(leftleftrightaabb.max()).containsExactly(5.0, 4.0);
        assertThat(leftleftright.start()).isEqualTo(1);
        assertThat(leftleftright.end()).isEqualTo(2);


        // RIGHT
        // (f:Node { point: [7.0, 2.0]}
        // (e:Node { point: [8.0, 1.0]}),
        // (c:Node { point: [9.0, 6.0]}),
        var right = root.rightChild();


        assertThat(right.start()).isEqualTo(3);
        assertThat(right.end()).isEqualTo(6);
        assertThat(kdTree.nodesContained(right)).containsExactly(
            graph.toMappedNodeId("f"),
            graph.toMappedNodeId("e"),
            graph.toMappedNodeId("c")
        );

        var rightAABB = (DoubleAABB) right.aabb();

        assertThat(rightAABB.min()).containsExactly(7.0, 1.0);
        assertThat(rightAABB.max()).containsExactly(9.0, 6.0);

        var rightright = right.rightChild();

        assertThat(rightright.isLeaf()).isTrue();
        assertThat(kdTree.nodesContained(rightright)).containsExactly(graph.toMappedNodeId("c"));
        var rightrightaabb = (DoubleAABB)rightright.aabb();
        assertThat(rightrightaabb.min()).containsExactly(9.0, 6.0);
        assertThat(rightrightaabb.max()).containsExactly(9.0, 6.0);
        assertThat(rightright.start()).isEqualTo(5);
        assertThat(rightright.end()).isEqualTo(6);

        var rightleft = right.leftChild();
        assertThat(kdTree.nodesContained(rightleft)).containsExactly(
            graph.toMappedNodeId("f"),
            graph.toMappedNodeId("e")
        );
        var rightleftaabb = (DoubleAABB) rightleft.aabb();
        assertThat(rightleftaabb.min()).containsExactly(7.0, 1.0);
        assertThat(rightleftaabb.max()).containsExactly(8.0, 2.0);
        assertThat(rightleft.start()).isEqualTo(3);
        assertThat(rightleft.end()).isEqualTo(5);


        var rightleftleft = rightleft.leftChild();
        var  rightleftleftaabb = (DoubleAABB)rightleftleft.aabb();
        assertThat(rightleftleft.isLeaf()).isTrue();
        assertThat(kdTree.nodesContained(rightleftleft)).containsExactly(graph.toMappedNodeId("f"));
        assertThat(rightleftleftaabb.min()).containsExactly(7.0, 2.0);
        assertThat(rightleftleftaabb.max()).containsExactly(7.0, 2.0);
        assertThat(rightleftleft.start()).isEqualTo(3);
        assertThat(rightleftleft.end()).isEqualTo(4);

        var rightleftright = rightleft.rightChild();
        var rightleftrightaabb = (DoubleAABB)rightleftright.aabb();
        assertThat(rightleftright.isLeaf()).isTrue();
        assertThat(kdTree.nodesContained(rightleftright)).containsExactly(graph.toMappedNodeId("e"));
        assertThat(rightleftrightaabb.min()).containsExactly(8.0, 1.0);
        assertThat(rightleftrightaabb.max()).containsExactly(8.0, 1.0);
        assertThat(rightleftright.start()).isEqualTo(4);
        assertThat(rightleftright.end()).isEqualTo(5);

        assertThat(kdTree.treeNodeCount()).isEqualTo(11);
    }

    @Test
    void shouldLogProgress(){

            var progressTask = HDBScanProgressTrackerCreator.kdBuildingTask("foo",graph.nodeCount());
            var log = new GdsTestLog();
            var progressTracker = new TaskProgressTracker(progressTask, new LoggerForProgressTrackingAdapter(log), new Concurrency(1), EmptyTaskRegistryFactory.INSTANCE);
            var points = graph.nodeProperties("point");

          new KdTreeBuilder(graph, points, 1, 1, null,progressTracker)
            .build();

            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                "foo :: Start",
                "foo 16%",
                "foo 33%",
                "foo 50%",
                "foo 66%",
                "foo 83%",
                "foo 100%",
                "foo :: Finished"
                );

    }

}
