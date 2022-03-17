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

/**
 * Computes the softmax for all classes except the last one which is
 * implicitly 1 - sum(output[i]) where i goes over all the other classes.
 */
public class ReducedSoftmax extends SingleParentVariable<Matrix, Matrix> {

    public ReducedSoftmax(Variable<Matrix> parent) {
        super(parent, Dimensions.matrix(parent.dimension(0), parent.dimension(1) + 1));
    }

    public static long sizeInBytes(int rows, int cols) {
        return Matrix.sizeInBytes(rows, cols - 1);
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        var data =  ctx.data(parent);
        int rows = data.rows();
        int cols = data.cols() + 1;

        var result = Matrix.create(0.0, rows, cols);
        boolean rescale = false;
        for (int row = 0; row < rows; row++) {
            double rowSum = 0;
            for (int col = 0; col < cols; col++) {
                var exp = col == cols - 1 ? 1.0 : Math.exp(data.dataAt(row, col));
                if (Double.isInfinite(exp)) {
                    rescale = true;
                    exp = Double.MAX_VALUE;
                }
                result.setDataAt(row, col, exp);
                rowSum += exp;
                if (Double.isInfinite(rowSum)) {
                    rescale = true;
                    rowSum = Double.MAX_VALUE;
                }
            }
            for (int col = 0; col < cols; col++) {
                var current = result.dataAt(row, col);
                result.setDataAt(row, col, current / rowSum);
            }
        }

        if (rescale) {
            rescale(result);
        }

        return result;
    }

    private static void rescale(Matrix result) {
        int rows = result.rows();
        int cols = result.cols();

        for (int row = 0; row < rows; row++) {
            double rowSum = 1e-15;
            for (int col = 0; col < cols; col++) {
                var index = row * cols + col;
                var current = result.dataAt(index);
                rowSum += current;
            }
            for (int col = 0; col < cols; col++) {
                var index = row * cols + col;
                var current = result.dataAt(index);
                result.setDataAt(index, current / rowSum);
            }
        }
    }

    // currently this class is only used with ReducedCrossEntropyLoss and then is not a parent,
    // so this gradient method of this class is never called
    @Override
    public Matrix gradientForParent(ComputationContext ctx) {
        var selfData = ctx.data(this);
        var selfGradient= ctx.gradient(this);

        int rows = selfData.rows();
        int cols = selfData.cols();

        var computedGradient = Matrix.create(0.0, rows, cols - 1);

        // result[row,col] = sum_{col2} s[row, col2] * (delta(col, col2) - s[row, col]) * grad[row, col2]
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols - 1; col++) {
                var softmaxData = selfData.dataAt(row, col);
                for (int softmaxCol = 0; softmaxCol < cols; softmaxCol++) {
                    double valueAtOtherColumn = selfData.dataAt(row, softmaxCol);
                    double gradientAtOtherColumn = selfGradient.dataAt(row, softmaxCol);
                    double impactOfChangingAnyColumn = (col == softmaxCol ? 1 : 0) - softmaxData;

                    double gradientValue = valueAtOtherColumn * impactOfChangingAnyColumn * gradientAtOtherColumn;
                    computedGradient.addDataAt(
                        row,
                        col,
                        gradientValue
                    );
                }
            }
        }
        return computedGradient;
    }
}
