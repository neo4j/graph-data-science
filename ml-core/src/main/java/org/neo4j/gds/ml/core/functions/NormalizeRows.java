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
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;

public class NormalizeRows extends SingleParentVariable<Matrix> {

    private static final double EPSILON = 1e-10;

    private final int rows;
    private final int cols;

    public NormalizeRows(Variable<Matrix> matrix) {
        super(matrix, matrix.dimensions());

        this.rows = dimension(0);
        this.cols = dimension(1);
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        double[] parentData = ctx.data(parent()).data();
        int rows = this.rows;
        int cols = this.cols;
        double[] result = new double[rows * cols];
        for (int row = 0; row < rows; row++) {
            double sum = 0;
            for (int col = 0; col < cols; col++) {
                int elementIndex = row * cols + col;
                sum += Math.pow(parentData[elementIndex], 2);
            }
            double l2 = Math.sqrt(sum);
            for (int col = 0; col < cols; col++) {
                int elementIndex = row * cols + col;
                result[elementIndex] = parentData[elementIndex] / (l2 + EPSILON);
            }
        }
        return new Matrix(result, this.rows, this.cols);
    }

    @Override
    public Matrix gradient(Variable<?> parent, ComputationContext ctx) {
        double[] parentData = ctx.data(parent).data();
        double[] gradientData = ctx.gradient(this).data();
        double[] result = new double[parentData.length];
        int rows = this.rows;
        int cols = this.cols;
        for (int row = 0; row < rows; row++) {
            double l2Squared = 0;
            for (int col = 0; col < cols; col++) {
                int elementIndex = row * cols + col;
                l2Squared += parentData[elementIndex] * parentData[elementIndex];
            }
            double l2 = Math.sqrt(l2Squared);
            double l2Cubed = l2 * l2Squared;
            for (int col = 0; col < cols; col++) {
                int elementIndex = row * cols + col;
                for (int gradCol = 0; gradCol < cols; gradCol++) {
                    if (col == gradCol) {
                        result[elementIndex] +=
                            gradientData[elementIndex] *
                            (l2Squared - parentData[elementIndex] * parentData[elementIndex]) / l2Cubed;
                    } else {
                        result[elementIndex] -=
                            gradientData[row * cols + gradCol] *
                            (parentData[elementIndex] * parentData[row * cols + gradCol]) / l2Cubed;
                    }
                }
            }
        }
        return new Matrix(result, this.rows, this.cols);
    }
}
