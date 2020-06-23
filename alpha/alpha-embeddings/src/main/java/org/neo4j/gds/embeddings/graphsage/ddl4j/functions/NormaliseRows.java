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

public class NormaliseRows extends SingleParentVariable {
    public NormaliseRows(Variable matrix) {
        super(matrix, matrix.dimensions());
    }

    @Override
    protected Tensor apply(ComputationContext ctx) {
        int rows = dimension(0);
        int vectorDim = dimension(1);
        double[] parentData = ctx.data(parent).data;
        double[] result = new double[rows * vectorDim];
        for (int row = 0; row < rows; row++) {
            double sum = 0;
            for (int col = 0; col < vectorDim; col++) {
                sum += Math.pow(parentData[row * vectorDim + col], 2);
            }
            double l2 = Math.sqrt(sum);
            for (int col = 0; col < vectorDim; col++) {
                result[row * vectorDim + col] = parentData[row * vectorDim + col] / l2;
            }
        }
        return Tensor.matrix(result, rows, vectorDim);
    }

    @Override
    protected Tensor gradient(ComputationContext ctx) {
        int rows = dimension(0);
        int cols = dimension(1);
        double[] parentData = ctx.data(parent).data;
        double[] gradientData = ctx.gradient(this).data;
        double[] result = new double[parentData.length];
        for (int row = 0; row < rows; row++) {
            double l2Squared = 0;
            for (int col = 0; col < cols; col++) {
                l2Squared += parentData[row * cols + col] * parentData[row * cols + col];
            }
            double l2 = Math.sqrt(l2Squared);
            double l2Qubed = l2 * l2Squared;
            for (int col = 0; col < cols; col++) {
                for (int gradCol = 0; gradCol < cols; gradCol++) {
                    if (col == gradCol) {
                        result[row * cols + col] += gradientData[row * cols + col] *
                                                    (l2Squared - parentData[row * cols + col] * parentData[row * cols + col]) / l2Qubed;
                    } else {
                        result[row * cols + col] -= gradientData[row * cols + gradCol] *
                                                    (parentData[row * cols + col] * parentData[row * cols + gradCol]) / l2Qubed;
                    }
                }
            }
        }
        return Tensor.matrix(result, rows, cols);
    }
}
