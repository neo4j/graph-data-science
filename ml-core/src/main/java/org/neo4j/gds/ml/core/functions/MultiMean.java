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
import org.neo4j.gds.ml.core.subgraph.BatchNeighbors;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;

public class MultiMean extends SingleParentVariable<Matrix> {
    private final BatchNeighbors subGraph;

    public MultiMean(
        Variable<Matrix> parent,
        BatchNeighbors subGraph
    ) {
        super(parent, Dimensions.matrix(subGraph.batchSize(), parent.dimension(1)));
        this.subGraph = subGraph;
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        Variable<?> parent = parent();
        Tensor<?> parentTensor = ctx.data(parent);
        double[] parentData = parentTensor.data();
        int[] batchIds = subGraph.batchIds();
        int batchSize = batchIds.length;

        int cols = parent.dimension(1);

        double[] means = new double[batchSize * cols];
        for (int batchIdx = 0; batchIdx < batchSize; batchIdx++) {
            int sourceId = batchIds[batchIdx];
            int batchIdRowOffset = sourceId * cols;
            int batchIdxOffset = batchIdx * cols;
            int[] neighbors = subGraph.neighbors(sourceId);
            int numberOfNeighbors = neighbors.length;

            for (int col = 0; col < cols; col++) {
                means[batchIdxOffset + col] += parentData[batchIdRowOffset + col] / (numberOfNeighbors + 1);
            }
            for (int targetIndex : neighbors) {
                int targetOffset = targetIndex * cols;
                double relationshipWeight = subGraph.relationshipWeight(sourceId, targetIndex);
                for (int col = 0; col < cols; col++) {
                    means[batchIdxOffset + col] += (parentData[targetOffset + col] * relationshipWeight) / (numberOfNeighbors + 1);
                }
            }
        }

        return new Matrix(means, batchSize, cols);
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        double[] multiMeanGradient = ctx.gradient(this).data();

        var batchIds = this.subGraph.batchIds();

        Tensor<?> result = ctx.data(parent).createWithSameDimensions();

        int cols = parent.dimension(1);

        for (int batchIdx = 0; batchIdx < batchIds.length; batchIdx++) {
            var sourceId = batchIds[batchIdx];
            int[] neighbors = subGraph.neighbors(sourceId);
            int degree = neighbors.length + 1;

            double[] cachedNeighborWeights = new double[neighbors.length];

            for (int i = 0; i < neighbors.length; i++) {
                cachedNeighborWeights[i] = subGraph.relationshipWeight(sourceId, neighbors[i]);
            }

            for (int col = 0; col < cols; col++) {
                int gradientElementIndex = batchIdx * cols + col;
                for (int neighborIndex = 0; neighborIndex < neighbors.length; neighborIndex++) {
                    int neighbor = neighbors[neighborIndex];
                    int neighborElementIndex = neighbor * cols + col;
                    result.addDataAt(
                        neighborElementIndex,
                        (1d / degree) * (multiMeanGradient[gradientElementIndex] * cachedNeighborWeights[neighborIndex])
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
