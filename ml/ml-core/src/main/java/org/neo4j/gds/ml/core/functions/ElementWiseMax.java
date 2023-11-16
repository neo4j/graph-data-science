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


public class ElementWiseMax extends SingleParentVariable<Matrix, Matrix> {
    private static final int INVALID_NEIGHBOR = -1;
    private final BatchNeighbors batchNeighbors;

    public ElementWiseMax(Variable<Matrix> parentVariable, BatchNeighbors batchGraph) {
        super(
            parentVariable,
            Dimensions.matrix(batchGraph.batchSize(), parentVariable.dimension(Dimensions.COLUMNS_INDEX))
        );
        this.batchNeighbors = batchGraph;

        assert parentVariable.dimension(Dimensions.ROWS_INDEX) >= batchGraph.nodeCount() : "Expecting a row for each node in the subgraph";
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        var parentData = ctx.data(parent);

        var rows = batchNeighbors.batchSize();
        var cols = parentData.cols();
        var batchIds = batchNeighbors.batchIds();

        var max = Matrix.create(Double.NEGATIVE_INFINITY, rows, cols);

        for (int batchIdx = 0; batchIdx < rows; batchIdx++) {
            // node-ids respond to rows in parentData
            int batchNodeId = batchIds[batchIdx];

            // Find the maximum value among the neighbors' data for each cell in the row
            for (int neighbor : batchNeighbors.neighbors(batchNodeId)) {
                double relationshipWeight = batchNeighbors.relationshipWeight(batchNodeId, neighbor);
                for (int col = 0; col < cols; col++) {
                    double neighborValue = parentData.dataAt(neighbor, col) * relationshipWeight;
                    if (neighborValue >= max.dataAt(batchIdx, col)) {
                        max.setDataAt(batchIdx, col, neighborValue);
                    }
                }
            }

            // Avoid Double.NEGATIVE_INFINITY entries for isolated batchNodes
            if (batchNeighbors.degree(batchNodeId) == 0) {
                for (int col = 0; col < cols; col++) {
                    max.setDataAt(batchIdx, col, 0);
                }
            }
        }

        return max;
    }

    @Override
    public Matrix gradientForParent(ComputationContext ctx) {
        var result = ctx.data(parent).createWithSameDimensions();

        var cols = result.cols();

        var parentData = ctx.data(parent);
        var elementWiseMaxGradient = ctx.gradient(this);
        var elementWiseMaxData = ctx.data(this);

        var batchIds = batchNeighbors.batchIds();
        for (int batchIdx = 0; batchIdx < batchNeighbors.batchSize(); batchIdx++) {
            int sourceId = batchIds[batchIdx];
            int[] neighbors = batchNeighbors.neighbors(sourceId);
            int degree = neighbors.length;
            double[] cachedWeights = new double[degree];

            for (int neighborIndex = 0; neighborIndex < degree; neighborIndex++) {
                cachedWeights[neighborIndex] = batchNeighbors.relationshipWeight(sourceId, neighbors[neighborIndex]);
            }

            for (int col = 0; col < cols; col++) {
                double thisCellData = elementWiseMaxData.dataAt(batchIdx, col);

                var minDiffToCellData = Double.MAX_VALUE;
                var maxNeighbor = INVALID_NEIGHBOR;
                var maxNeighborWeight = Double.NaN;

                // Find neighbor that had contributed the max value
                for (int neighborIndex = 0; neighborIndex < degree; neighborIndex++) {
                    int neighbor = neighbors[neighborIndex];
                    double relationshipWeight = cachedWeights[neighborIndex];

                    var diffToCellData = Math.abs(
                        thisCellData - (parentData.dataAt(neighbor, col) * relationshipWeight)
                    );

                    if (diffToCellData < minDiffToCellData) {
                        minDiffToCellData = diffToCellData;
                        maxNeighbor = neighbor;
                        maxNeighborWeight = relationshipWeight;
                    }
                }

                if (maxNeighbor == INVALID_NEIGHBOR) {
                    assert degree == 0;
                    continue;
                }

                // propagate gradient to the neighbors' cell
                result.addDataAt(maxNeighbor, col, elementWiseMaxGradient.dataAt(batchIdx, col) * maxNeighborWeight);
            }
        }

        return result;
    }
}
