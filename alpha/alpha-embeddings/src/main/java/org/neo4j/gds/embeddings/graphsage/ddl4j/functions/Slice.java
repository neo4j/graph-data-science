/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;

public class Slice extends SingleParentVariable implements Matrix {

    private final int[] selfAdjacency;
    private final int rows;
    private final int cols;

    public Slice(Variable parent, int[] selfAdjacencyMatrix) {
        super(parent, Dimensions.matrix(selfAdjacencyMatrix.length, parent.dimension(1)));

        this.selfAdjacency = selfAdjacencyMatrix;
        this.rows = selfAdjacencyMatrix.length;
        this.cols = parent.dimension(1);
    }

    @Override
    public Tensor apply(ComputationContext ctx) {
        double[] parentData = ctx.data(parent()).data;

        double[] result = new double[rows * cols];

        for (int row = 0; row < rows; row++) {
            System.arraycopy(parentData, selfAdjacency[row] * cols, result, row * cols, cols);
        }

        return Tensor.matrix(result, rows, cols);
    }

    @Override
    public Tensor gradient(Variable parent, ComputationContext ctx) {
        Tensor result = ctx.data(parent).zeros();

        double[] selfGradient = ctx.gradient(this).data;
        for (int row = 0; row < rows; row++) {
            int childRow = selfAdjacency[row];
            for (int col = 0; col < cols; col++) {
                result.data[childRow * cols + col] += selfGradient[row * cols + col];
            }
        }

        return result;
    }

    @Override
    public int rows() {
        return rows;
    }

    @Override
    public int cols() {
        return cols;
    }
}
