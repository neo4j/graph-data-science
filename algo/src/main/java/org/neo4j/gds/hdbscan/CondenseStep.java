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
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

class CondenseStep {
    private final long nodeCount;
    private final ProgressTracker progressTracker;

    CondenseStep(long nodeCount, ProgressTracker progressTracker) {
        this.nodeCount = nodeCount;
        this.progressTracker = progressTracker;
    }

    CondensedTree condense(ClusterHierarchy clusterHierarchy, long minClusterSize) {

        // Walk through the hierarchy
        //      at each split if one of the clusters created by the split has fewer points than the minimum cluster size
        //          if it is the case that we have fewer points than the minimum cluster size we declare it to be
        //              ‘points falling out of a cluster’
        //          the larger cluster retain the cluster identity of the parent,
        //              marking down which points ‘fell out of the cluster’ and at what distance value that happened.
        //
        //      If the split is into two clusters each at least as large as the minimum cluster size
        //          then we consider that a true cluster split and let that split persist in the tree.
        //
        // After walking through the whole hierarchy and doing this we end up
        // with a much smaller tree with a small number of nodes,
        // each of which has data about how the size of the cluster at that node decreases over varying distance.
        progressTracker.beginSubTask();
        var clusterHierarchyRoot = clusterHierarchy.root();
        var parent = HugeLongArray.newArray(clusterHierarchyRoot + 1);
        var lambda = HugeDoubleArray.newArray(clusterHierarchyRoot + 1);
        var size = HugeLongArray.newArray(nodeCount);


        var relabel = HugeLongArray.newArray(nodeCount);
        var currentCondensedRoot = nodeCount;
        relabel.set(clusterHierarchyRoot - nodeCount, currentCondensedRoot);

        var currentCondensedMaxClusterId = nodeCount;
        var bfsQueue = HugeLongArrayQueue.newQueue(nodeCount);
        var visited = new BitSet(clusterHierarchyRoot + 1);

        size.set(currentCondensedRoot - nodeCount, nodeCount);

        for (var i = clusterHierarchyRoot; i >= nodeCount; i--) {
            if (visited.get(i)) {
                continue;
            }

            var left = clusterHierarchy.left(i);
            var leftSize = clusterHierarchy.size(left);
            var right = clusterHierarchy.right(i);
            var rightSize = clusterHierarchy.size(right);

            var currentReLabel = relabel.get(i - nodeCount);
            var fallingOutLambda = clusterHierarchy.lambda(i);
            if (leftSize < minClusterSize && rightSize < minClusterSize) { // both fall out of cluster
                fallOut(clusterHierarchy, left, parent, currentReLabel, lambda, fallingOutLambda, bfsQueue, visited);
                fallOut(clusterHierarchy, right, parent, currentReLabel, lambda, fallingOutLambda, bfsQueue, visited);

            } else if (leftSize < minClusterSize && rightSize >= minClusterSize) { // left falls out, right retains parent cluster id
                fallOut(clusterHierarchy, left, parent, currentReLabel, lambda, fallingOutLambda, bfsQueue, visited);
                relabel.set(right - nodeCount, currentReLabel);
            } else if (leftSize >= minClusterSize && rightSize < minClusterSize) { // left retains parent cluster id, right falls out
                relabel.set(left - nodeCount, currentReLabel);

                fallOut(clusterHierarchy, right, parent, currentReLabel, lambda, fallingOutLambda, bfsQueue, visited);

            } else { // none fall out, both get new cluster ids
                var leftClusterId = ++currentCondensedMaxClusterId;
                relabel.set(left - nodeCount, leftClusterId);
                parent.set(leftClusterId, currentReLabel);
                lambda.set(leftClusterId, fallingOutLambda);
                size.set(leftClusterId - nodeCount, leftSize);

                var rightClusterId = ++currentCondensedMaxClusterId;
                relabel.set(right - nodeCount, rightClusterId);
                parent.set(rightClusterId, currentReLabel);
                lambda.set(rightClusterId, fallingOutLambda);
                size.set(rightClusterId - nodeCount, rightSize);
            }
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();

        return new CondensedTree(currentCondensedRoot, parent, lambda, size, currentCondensedMaxClusterId, nodeCount);
    }

    private void fallOut(
        ClusterHierarchy clusterHierarchy,
        long nodeToFallOut,
        HugeLongArray parent,
        long clusterToFallOutFrom,
        HugeDoubleArray lambda,
        double fallingOutLambda,
        HugeLongArrayQueue bfsQueue,
        BitSet visited
    ) {
        if (nodeToFallOut < nodeCount) {
            parent.set(nodeToFallOut, clusterToFallOutFrom);
            lambda.set(nodeToFallOut, fallingOutLambda);
        } else {
            // for each descendant of nodeToFallOut - find the leaf (original node) and mark it as fallen out
            bfsQueue.add(nodeToFallOut);
            while (!bfsQueue.isEmpty()) {
                var currentNode = bfsQueue.remove();
                var left = clusterHierarchy.left(currentNode);
                var right = clusterHierarchy.right(currentNode);

                if (left < nodeCount) {
                    parent.set(left, clusterToFallOutFrom);
                    lambda.set(left, fallingOutLambda);
                } else {
                    bfsQueue.add(left);
                }

                if (right < nodeCount) {
                    parent.set(right, clusterToFallOutFrom);
                    lambda.set(right, fallingOutLambda);
                } else {
                    bfsQueue.add(right);
                }
                visited.set(currentNode);
            }
        }

    }
}
