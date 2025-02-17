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

class StabilityStep {
    HugeDoubleArray computeStabilities(CondensedTree condensedTree, long nodeCount) {
        var result = HugeDoubleArray.newArray(nodeCount - 1);

        var condensedTreeRoot = condensedTree.root();
        // process the leaves of the tree
        for (var p = 0; p < nodeCount; p++) {
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

    BitSet selectedClusters(CondensedTree condensedTree, HugeDoubleArray stabilities, long nodeCount) {

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
}
