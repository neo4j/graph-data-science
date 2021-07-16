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
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;

public class MultiMean extends SingleParentVariable<Matrix> {
    private final int[][] adjacency;
    private final int[] selfAdjacency;
    private final int rows;
    private final int cols;

    public MultiMean(
        Variable<?> parent,
        int[][] adjacency,
        int[] selfAdjacency
    ) {
        super(parent, Dimensions.matrix(adjacency.length, parent.dimension(1)));
        this.adjacency = adjacency;
        this.selfAdjacency = selfAdjacency;
        this.rows = adjacency.length;
        this.cols = parent.dimension(1);
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        Variable<?> parent = parent();
        Tensor<?> parentTensor = ctx.data(parent);
        double[] parentData = parentTensor.data();
        double[] means = new double[adjacency.length * cols];
        for (int source = 0; source < adjacency.length; source++) {
            int selfAdjacencyOfSourceOffset = selfAdjacency[source] * cols;
            int sourceOffset = source * cols;
            int[] neighbors = adjacency[source];
            int numberOfNeighbors = neighbors.length;
            for (int col = 0; col < cols; col++) {
                means[sourceOffset + col] += parentData[selfAdjacencyOfSourceOffset + col] / (numberOfNeighbors + 1);
            }
            for (int target : neighbors) {
                int targetOffset = target * cols;
                for (int col = 0; col < cols; col++) {
                    means[sourceOffset + col] += parentData[targetOffset + col] / (numberOfNeighbors + 1);
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
                int degree = adjacency[row].length + 1;
                int gradientElementIndex = row * cols + col;
                for (int neighbor : adjacency[row]) {
                    int neighborElementIndex = neighbor * cols + col;
                    result.addDataAt(
                        neighborElementIndex,
                        1d / degree * multiMeanGradient[gradientElementIndex]
                    );
                }
                result.addDataAt(
                    selfAdjacency[row] * cols + col,
                    1d / degree * multiMeanGradient[gradientElementIndex]
                );
            }
        }

        return result;
    }
}
