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
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;

import java.util.List;

public class BroadcastSum extends Variable {

    private final Variable matrix;
    private final Variable vector;

    public BroadcastSum(Variable matrix, Variable vector) {
        super(List.of(matrix, vector), matrix.dimensions());

        this.matrix = matrix;
        this.vector = vector;
    }

    @Override
    protected Tensor apply(ComputationContext ctx) {

        double[] matrixData = ctx.data(matrix).data;
        double[] vectorData = ctx.data(vector).data;

        double[] result = new double[matrixData.length];

        int rows = dimension(0);
        int cols = dimension(1);

        for(int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                int matrixIndex = row * cols + col;
                result[matrixIndex] = matrixData[matrixIndex] + vectorData[col];
            }
        }

        return Tensor.matrix(result, rows, cols);
    }

    @Override
    protected Tensor gradient(Variable parent, ComputationContext ctx) {
        if (parent == matrix) {
            return ctx.gradient(this);
        } else {
            Tensor gradient = ctx.gradient(this);
            int rows = dimension(0);
            int cols = vector.dimension(0);
            double[] result = new double[cols];
            for (int row = 0; row < rows; row++) {
                for (int col = 0; col < cols; col++) {
                    int matrixIndex = row * cols + col;
                    result[col] += gradient.data[matrixIndex];
                }
            }

            return Tensor.vector(result);
        }
    }
}
