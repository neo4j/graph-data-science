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

public class DecisionTreePredict<P> {

    private final TreeNode<P> root;

    DecisionTreePredict(TreeNode<P> root) {
        this.root = root;
    }

    public P predict(double[] features) {
        assert features.length > 0;

        TreeNode<P> node = root;

        while (node.leftChild != null) {
            assert features.length > node.index;
            assert node.rightChild != null;

            if (features[node.index] < node.value) {
                node = node.leftChild;
            } else {
                node = node.rightChild;
            }
        }

        return node.prediction;
    }
}
