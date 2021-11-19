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

import org.neo4j.gds.core.utils.DoubleUtil;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.subgraph.BatchNeighbors;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;


/*
    Column-wise, element-wise maximum
        Parent matrix       n x m
        Adjacency matrix    p x q
        Result              p x m

    Assumption:
        Neighbour node IDs are smaller than the row count of the parent matrix

    int[nodes][neighbours] adjacencyMatrix
 */
public class ElementWiseMax extends SingleParentVariable<Matrix> {
    private final Variable<Matrix> parent;
    private final BatchNeighbors batchNeighbors;

    public ElementWiseMax(Variable<Matrix> parent, BatchNeighbors batchGraph) {
        super(parent, Dimensions.matrix(batchGraph.batchSize(), parent.dimension(Dimensions.COLUMNS_INDEX)));
        this.batchNeighbors = batchGraph;
        this.parent = parent;
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        var parentData = ctx.data(parent);

        var rows = batchNeighbors.batchSize();
        var cols = parentData.cols();

        var max = Matrix.create(Double.NEGATIVE_INFINITY, rows, cols);

        for (int row = 0; row < rows; row++) {
            int[] neighbors = this.batchNeighbors.neighbors(row);
            for(int col = 0; col < cols; col++) {
                if (neighbors.length > 0) {
                    for (int neighbor : neighbors) {
                        max.setDataAt(row, col, Math.max(parentData.dataAt(neighbor, col), max.dataAt(row, col)));
                    }
                } else {
                    max.setDataAt(row, col, 0);
                }
            }
        }

        return max;
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        var result = (Matrix) ctx.data(parent).createWithSameDimensions();

        var rows = this.batchNeighbors.batchSize();
        var cols = result.cols();

        var parentData = (Matrix) ctx.data(parent);
        var thisGradient = ctx.gradient(this);
        var thisData = ctx.data(this);

        for (int row = 0; row < rows; row++) {
            int[] neighbors = this.batchNeighbors.neighbors(row);
            for (int col = 0; col < cols; col++) {
                for (int neighbor : neighbors) {
                    if (DoubleUtil.compareWithDefaultThreshold(parentData.dataAt(neighbor, col), thisData.dataAt(row, col))) {
                        result.addDataAt(neighbor, col, thisGradient.dataAt(row, col));
                    }
                }
            }
        }

        return result;
    }
}
