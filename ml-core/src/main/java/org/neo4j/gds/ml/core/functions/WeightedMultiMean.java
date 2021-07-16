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
package org.neo4j.gds.ml.core.functions;

import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.RelationshipWeights;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.subgraph.SubGraph;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;

public class WeightedMultiMean extends SingleParentVariable<Matrix> {
    private final RelationshipWeights relationshipWeights;
    private final SubGraph subGraph;
    private final int[][] adjacency;
    private final int[] selfAdjacency;
    private final int rows;
    private final int cols;

    public WeightedMultiMean(
        Variable<Matrix> parent,
        RelationshipWeights relationshipWeights,
        SubGraph subGraph
    ) {
        super(parent, Dimensions.matrix(subGraph.adjacency.length, parent.dimension(1)));
        this.relationshipWeights = relationshipWeights;
        this.subGraph = subGraph;
        this.adjacency = subGraph.adjacency;
        this.selfAdjacency = subGraph.selfAdjacency;
        this.rows = adjacency.length;
        this.cols = parent.dimension(Dimensions.COLUMNS_INDEX);
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        Variable<?> parent = parent();
        Tensor<?> parentTensor = ctx.data(parent);
        double[] parentData = parentTensor.data();
        double[] means = new double[adjacency.length * cols];
        for (int sourceIndex = 0; sourceIndex < adjacency.length; sourceIndex++) {
            int sourceId = selfAdjacency[sourceIndex];
            long originalSourceId = subGraph.nextNodes[sourceId];
            int selfAdjacencyOfSourceOffset = sourceId * cols;
            int sourceOffset = sourceIndex * cols;
            int[] neighbors = adjacency[sourceIndex];
            int numberOfNeighbors = neighbors.length;
            for (int col = 0; col < cols; col++) {
                means[sourceOffset + col] += parentData[selfAdjacencyOfSourceOffset + col] / (numberOfNeighbors + 1);
            }
            for (int targetIndex : neighbors) {
                int targetOffset = targetIndex * cols;
                long originalTargetId = subGraph.nextNodes[targetIndex];
                double relationshipWeight = relationshipWeights.weight(originalSourceId, originalTargetId);
                for (int col = 0; col < cols; col++) {
                    means[sourceOffset + col] += (parentData[targetOffset + col] * relationshipWeight) / (numberOfNeighbors + 1);
                }
            }
        }

        return new Matrix(means, this.rows, this.cols);
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        double[] multiMeanGradient = ctx.gradient(this).data();

        Tensor<?> result = ctx.data(parent).createWithSameDimensions();

        for (int col = 0; col < cols; col++) {
            for (int row = 0; row < rows; row++) {
                int sourceId = selfAdjacency[row];
                long originalSourceId = subGraph.nextNodes[sourceId];

                int degree = adjacency[row].length + 1;
                int gradientElementIndex = row * cols + col;
                for (int neighbor : adjacency[row]) {
                    long originalTargetId = subGraph.nextNodes[neighbor];
                    double relationshipWeight = relationshipWeights.weight(originalSourceId, originalTargetId); //TODO normalize weights
                    int neighborElementIndex = neighbor * cols + col;
                    result.addDataAt(
                        neighborElementIndex,
                        (1d / degree) * (multiMeanGradient[gradientElementIndex] * relationshipWeight)
                    );
                }
                result.addDataAt(
                    sourceId * cols + col,
                    (1d / degree) * multiMeanGradient[gradientElementIndex]
                );
            }
        }

        return result;
    }
}
