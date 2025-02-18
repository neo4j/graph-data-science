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

class LabellingStep {

    private final CondensedTree condensedTree;
    private final long nodeCount;

    LabellingStep(CondensedTree condensedTree, long nodeCount) {
        this.condensedTree = condensedTree;
        this.nodeCount = nodeCount;
    }

    HugeDoubleArray computeStabilities() {
        var result = HugeDoubleArray.newArray(nodeCount - 1);

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
        }

        return result;
    }

    BitSet selectedClusters(HugeDoubleArray stabilities) {

        var selectedClusters = new BitSet(nodeCount);

        var condensedTreeRoot = condensedTree.root();
        var condensedTreeMaxClusterId = condensedTree.maximumClusterId();

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
            if (p == condensedTreeRoot) {
                continue;
            }
            var parent = condensedTree.parent(p);
            stabilitySums.addTo(parent - nodeCount, stabilityToAdd);
        }

        return selectedClusters;
    }

    HugeLongArray computeLabels(BitSet selectedClusters) {
        var labels = HugeLongArray.newArray(nodeCount);
        labels.fill(-1L);
        var nodeCountLabels = HugeLongArray.newArray(nodeCount);
        var root = condensedTree.root();
        var maximumClusterId = condensedTree.maximumClusterId();
        for (var p = root; p <= maximumClusterId; p++) {
            var adaptedIndex = p - nodeCount;
            var parent = condensedTree.parent(p);
            long parentLabel = p == root ? -1L : labels.get(parent - nodeCount);
            if (parentLabel != -1L) {
                labels.set(adaptedIndex, parentLabel);
            } else if (selectedClusters.get(adaptedIndex)) {
                labels.set(adaptedIndex, adaptedIndex);
            }
        }

        for (var n = 0; n < nodeCount; n++) {
            nodeCountLabels.set(n, labels.get(condensedTree.fellOutOf(n) - nodeCount));
        }

        return nodeCountLabels;
    }

    HugeLongArray labels() {
        var stabilities = computeStabilities();
        var selectedClusters = selectedClusters(stabilities);
        return computeLabels(selectedClusters);
    }
}
