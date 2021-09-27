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

public class ClassificationDecisionTree<L extends DecisionTreeLoss> extends DecisionTree<L, Integer> {

    private final int[] classes;
    private final int[] allLabels;

    public ClassificationDecisionTree(
        L lossFunction,
        double[][] allFeatures,
        int maxDepth,
        int minSize,
        int[] classes,
        int[] allLabels
    ) {
        super(lossFunction, allFeatures, maxDepth, minSize);

        assert classes.length > 0;
        this.classes = classes;

        assert allLabels.length == allFeatures.length;
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
    protected Integer toTerminal(int[] group, int groupSize) {
        assert groupSize > 0;
        assert group.length >= groupSize;

        var classesInGroup = new int[classes.length];

        for (int i = 0; i < groupSize; i++) {
            classesInGroup[allLabels[group[i]]]++;
        }

        int max = -1;
        int maxClass = 0;
        for (int i = 0; i < classesInGroup.length; i++) {
            if (classesInGroup[i] <= max) continue;

            max = classesInGroup[i];
            maxClass = i;
        }

        return maxClass;
    }
}
