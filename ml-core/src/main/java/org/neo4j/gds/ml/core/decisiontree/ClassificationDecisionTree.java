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
package org.neo4j.gds.ml.core.decisiontree;

import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

public class ClassificationDecisionTree<L extends DecisionTreeLoss> extends DecisionTree<L, Integer> {

    private final int[] classes;
    private final HugeIntArray allLabels;

    public ClassificationDecisionTree(
        AllocationTracker allocationTracker,
        L lossFunction,
        HugeObjectArray<double[]> allFeatures,
        int maxDepth,
        int minSize,
        int[] classes,
        HugeIntArray allLabels
    ) {
        super(allocationTracker, lossFunction, allFeatures, maxDepth, minSize);

        assert classes.length > 0;
        this.classes = classes;

        assert allLabels.size() == allFeatures.size();
        this.allLabels = allLabels;
    }

    @Override
    public Integer predict(TreeNode node, double[] features) {
        assert features.length > 0;

        if (node instanceof LeafNode) {
            return ((LeafNode<Integer>) node).prediction();
        }

        var nonLeafNode = (NonLeafNode) node;
        if (features[nonLeafNode.index()] < nonLeafNode.value()) {
            return predict(nonLeafNode.leftChild(), features);
        } else {
            return predict(nonLeafNode.rightChild(), features);
        }
    }

    @Override
    protected Integer toTerminal(HugeLongArray group, long groupSize) {
        assert groupSize > 0;
        assert group.size() >= groupSize;

        var classesInGroup = new long[classes.length];

        for (long i = 0; i < groupSize; i++) {
            classesInGroup[allLabels.get(group.get(i))]++;
        }

        long max = -1;
        int maxClass = 0;
        for (int i = 0; i < classesInGroup.length; i++) {
            if (classesInGroup[i] <= max) continue;

            max = classesInGroup[i];
            maxClass = i;
        }

        return maxClass;
    }
}
