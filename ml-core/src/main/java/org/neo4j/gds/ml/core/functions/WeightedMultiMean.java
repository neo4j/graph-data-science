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
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.subgraph.SubGraph;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;

public class WeightedMultiMean extends SingleParentVariable<Matrix> {
    private final SubGraph subGraph;
    private final int[] selfAdjacency;
    private final int rows;
    private final int cols;

    public WeightedMultiMean(
        Variable<Matrix> parent,
        SubGraph subGraph
    ) {
        super(parent, Dimensions.matrix(subGraph.batchSize(), parent.dimension(1)));
        this.subGraph = subGraph;
        this.selfAdjacency = subGraph.mappedBatchedNodeIds;
        this.rows = subGraph.batchSize();
        this.cols = parent.dimension(Dimensions.COLUMNS_INDEX);
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        Variable<?> parent = parent();
        Tensor<?> parentTensor = ctx.data(parent);
        double[] parentData = parentTensor.data();
        int batchSize = this.subGraph.batchSize();
        double[] means = new double[batchSize * cols];
        for (int sourceIndex = 0; sourceIndex < batchSize; sourceIndex++) {
            int sourceId = selfAdjacency[sourceIndex];
            int selfAdjacencyOfSourceOffset = sourceId * cols;
            int sourceOffset = sourceIndex * cols;
            int[] neighbors = this.subGraph.neighbors(sourceIndex);
            int numberOfNeighbors = neighbors.length;
            for (int col = 0; col < cols; col++) {
                means[sourceOffset + col] += parentData[selfAdjacencyOfSourceOffset + col] / (numberOfNeighbors + 1);
            }
            for (int targetIndex : neighbors) {
                int targetOffset = targetIndex * cols;
                double relationshipWeight = subGraph.relWeight(sourceId, targetIndex);
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

                int[] neighbors = this.subGraph.neighbors(row);
                int degree = neighbors.length + 1;
                int gradientElementIndex = row * cols + col;
                for (int neighbor : neighbors) {
                    double relationshipWeight = subGraph.relWeight(sourceId, neighbor); //TODO normalize weights
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
