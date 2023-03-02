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
package org.neo4j.gds.ml.decisiontree;

import java.util.Objects;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class TreeNode<PREDICTION extends Number> {
    private PREDICTION prediction;
    private int featureIndex = -1;
    private double thresholdValue;
    private TreeNode<PREDICTION> leftChild = null;
    private TreeNode<PREDICTION> rightChild = null;

    public TreeNode(int index, double value) {
        assert index >= 0;

        this.featureIndex = index;
        this.thresholdValue = value;
    }

    public static long splitMemoryEstimation() {
        return sizeOfInstance(TreeNode.class);
    }

    public static <T extends Number> long leafMemoryEstimation(Class<T> leafType) {
        return sizeOfInstance(TreeNode.class) + sizeOfInstance(leafType);
    }

    public TreeNode(PREDICTION prediction) {
        this.prediction = prediction;
    }

    public void setPrediction(PREDICTION prediction) { this.prediction = prediction; }

    public PREDICTION prediction() {
        return prediction;
    }

    public int featureIndex() {
        return featureIndex;
    }

    public double thresholdValue() {
        return thresholdValue;
    }

    public TreeNode<PREDICTION> leftChild() {
        return leftChild;
    }

    public void setLeftChild(TreeNode leftChild) {
        this.leftChild = leftChild;
    }

    public boolean hasLeftChild() { return leftChild != null; }

    public TreeNode<PREDICTION> rightChild() {
        return rightChild;
    }

    public void setRightChild(TreeNode rightChild) {
        this.rightChild = rightChild;
    }

    public boolean hasRightChild() { return rightChild != null; }

    @Override
    public String toString() {
        return formatWithLocale("Node: prediction %s, featureIndex %s, splitValue %f", this.prediction, this.featureIndex, this.thresholdValue);
    }

    /**
     * Renders the variable into a human readable representation.
     */
    public String render() {
        StringBuilder sb = new StringBuilder();
        render(sb, this, 0);
        return sb.toString();
    }


    static void render(StringBuilder sb, TreeNode<?> node, int depth) {
        if (node == null) {
            return;
        }

        sb.append("\t".repeat(Math.max(0, depth - 1)));

        if (depth > 0) {
            sb.append("|-- ");
        }

        sb.append(node);
        sb.append(System.lineSeparator());

        render(sb, node.leftChild, depth + 1);
        render(sb, node.rightChild, depth + 1);
    }

    private static boolean equals(TreeNode o, TreeNode b) {
        if (o == null && b != null) return false;
        if (o != null && b == null) return false;
        if (o == null && b == null) return true;
        return o.featureIndex == b.featureIndex && Double.compare(o.thresholdValue, b.thresholdValue) == 0
               && o.prediction.equals(b.prediction) && TreeNode.equals(o.leftChild, b.leftChild) && TreeNode.equals(o.rightChild, b.rightChild);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || TreeNode.class != o.getClass()) return false;
        TreeNode<PREDICTION> treeNode = (TreeNode<PREDICTION>) o;
        return equals(this, treeNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prediction, featureIndex, thresholdValue, leftChild, rightChild);
    }
}
