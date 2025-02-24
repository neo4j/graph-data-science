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
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

class LabellingStep {

    private final CondensedTree condensedTree;
    private final long nodeCount;
    private final ProgressTracker progressTracker;
    private static final long NOISE = -1;

    LabellingStep(CondensedTree condensedTree, long nodeCount, ProgressTracker progressTracker) {
        this.condensedTree = condensedTree;
        this.nodeCount = nodeCount;
        this.progressTracker = progressTracker;
    }

    HugeDoubleArray computeStabilities() {
        var result = HugeDoubleArray.newArray(nodeCount - 1);
        progressTracker.beginSubTask();
        var condensedTreeRoot = this.condensedTree.root();
        // process the leaves of the tree
        for (var p = 0; p < this.nodeCount; p++) {
            var lambdaP = 1. / condensedTree.lambda(p);
            var birthPoint = condensedTree.fellOutOf(p);
            var lambdaBirth = birthPoint == condensedTreeRoot
                ? 0.
                : 1. / condensedTree.lambda(birthPoint);
            result.addTo(birthPoint - nodeCount, lambdaP - lambdaBirth);
        }

        var condensedTreeMaxClusterId = condensedTree.maximumClusterId();
        for (var p = condensedTreeMaxClusterId; p > condensedTreeRoot; p--) {
            var lambdaP = 1. / condensedTree.lambda(p);
            var birthPoint = condensedTree.parent(p);
            var lambdaBirth = birthPoint == condensedTreeRoot
                ? 0.
                : 1. / condensedTree.lambda(birthPoint);
            var sizeP = condensedTree.size(p);
            result.addTo(birthPoint - nodeCount, sizeP * (lambdaP - lambdaBirth));
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();
        return result;
    }

    BitSet selectedClusters(HugeDoubleArray stabilities) {

        var selectedClusters = new BitSet(nodeCount);

        var condensedTreeRoot = condensedTree.root();
        var condensedTreeMaxClusterId = condensedTree.maximumClusterId();
        progressTracker.beginSubTask();
        var stabilitySums = HugeDoubleArray.newArray(nodeCount);
        for (var p = condensedTreeMaxClusterId; p >= condensedTreeRoot; p--) {
            var adaptedPIndex = p - nodeCount;
            var stabilityP = stabilities.get(adaptedPIndex);
            var childrenStabilitySum = stabilitySums.get(adaptedPIndex);
            double stabilityToAdd;
            if (childrenStabilitySum > stabilityP) {
                stabilityToAdd = childrenStabilitySum;
                selectedClusters.clear(adaptedPIndex);
            } else {
                stabilityToAdd = stabilityP;
                selectedClusters.set(adaptedPIndex);
                // Selected clusters below `p` are implicitly unselected - they will be ignored during- `labeling`
            }
            progressTracker.logProgress();
            if (p == condensedTreeRoot) {
                continue;
            }
            var parent = condensedTree.parent(p);
            stabilitySums.addTo(parent - nodeCount, stabilityToAdd);
        }
        progressTracker.endSubTask();
        return selectedClusters;
    }

    Labels computeLabels(BitSet selectedClusters) {
        progressTracker.beginSubTask();
        var treeLabels = HugeLongArray.newArray(nodeCount);
        var labels = HugeLongArray.newArray(nodeCount);
        treeLabels.fill(NOISE);
        long clusters=0;
        var root = condensedTree.root();
        var maximumClusterId = condensedTree.maximumClusterId();
        for (var p = root; p <= maximumClusterId; p++) {
            var adaptedIndex = p - nodeCount;
            var parent = condensedTree.parent(p);
            long parentLabel = p == root ? NOISE : treeLabels.get(parent - nodeCount);
            if (parentLabel != NOISE) {
                treeLabels.set(adaptedIndex, parentLabel);
            } else if (selectedClusters.get(adaptedIndex)) {
                clusters++;
                treeLabels.set(adaptedIndex, adaptedIndex);
            }
            progressTracker.logProgress();
        }

        long noisePoints=0;
        for (var n = 0; n < nodeCount; n++) {
            long label = treeLabels.get(condensedTree.fellOutOf(n) - nodeCount);
            if (label == NOISE)  {
                noisePoints++;
            }
            labels.set(n, label);
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();

        return new Labels(labels,noisePoints,clusters);
    }

    Labels labels() {
        progressTracker.beginSubTask();
        var stabilities = computeStabilities();
        var selectedClusters = selectedClusters(stabilities);
        var labels= computeLabels(selectedClusters);
        progressTracker.endSubTask();
        return labels;
    }
}
