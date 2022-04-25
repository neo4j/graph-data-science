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

public class NormalizeRows extends SingleParentVariable<Matrix, Matrix> {

    private static final double EPSILON = 1e-10;

    public NormalizeRows(Variable<Matrix> matrix) {
        super(matrix, matrix.dimensions());
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        var parentMatrix = ctx.data(parent);
        int rows = parentMatrix.rows();
        int cols = parentMatrix.cols();

        var result = parentMatrix.createWithSameDimensions();
        for (int row = 0; row < rows; row++) {
            double squaredSum = 0;
            for (int col = 0; col < cols; col++) {
                squaredSum += parentMatrix.dataAt(row, col) * parentMatrix.dataAt(row, col);
            }

            // adding EPSILON to avoid division by zero
            double l2 = Math.sqrt(squaredSum) + EPSILON;
            for (int col = 0; col < cols; col++) {
                result.setDataAt(row, col, parentMatrix.dataAt(row, col) / l2);
            }
        }
        return result;
    }

    @Override
    public Matrix gradientForParent(ComputationContext ctx) {
        Matrix parentData = ctx.data(parent);
        Matrix normalizeRowsGradient = ctx.gradient(this);

        Matrix parentGradient = parentData.createWithSameDimensions();
        int rows = parentData.rows();
        int cols = parentData.cols();

        for (int row = 0; row < rows; row++) {
            double l2Squared = 0;
            for (int col = 0; col < cols; col++) {
                double cellValue = parentData.dataAt(row, col);
                l2Squared += cellValue * cellValue;
            }
            double l2 = Math.sqrt(l2Squared);
            double l2Cubed = l2 * l2Squared;

            if (Double.compare(l2Cubed, 0) == 0) {
                continue;
            }

            for (int col = 0; col < cols; col++) {
                double parentCellValue = parentData.dataAt(row, col);
                for (int gradCol = 0; gradCol < cols; gradCol++) {
                    double partialGradient;
                    if (col == gradCol) {
                        partialGradient = normalizeRowsGradient.dataAt(row, col) * (l2Squared - parentCellValue * parentCellValue);
                    } else {
                        partialGradient = -normalizeRowsGradient.dataAt(row, gradCol) * (parentCellValue * parentData.dataAt(row, gradCol));
                    }
                    parentGradient.addDataAt(row, col, partialGradient);
                }

                parentGradient.setDataAt(row, col, parentGradient.dataAt(row, col) / l2Cubed);
            }
        }

        return parentGradient;
    }
}
