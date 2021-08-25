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

import org.neo4j.gds.ml.core.AbstractVariable;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;

import java.util.List;

import static org.neo4j.gds.ml.core.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.ml.core.Dimensions.ROWS_INDEX;

public class MatrixScalarSum extends AbstractVariable<Matrix> {

    private final Variable<Matrix> matrix;
    private final Variable<Scalar> scalar;
    private final int rows;
    private final int cols;

    public MatrixScalarSum(Variable<Matrix> matrix, Variable<Scalar> scalar) {
        super(List.of(matrix, scalar), matrix.dimensions());
        this.matrix = matrix;
        this.rows = matrix.dimension(ROWS_INDEX);
        this.cols = matrix.dimension(COLUMNS_INDEX);
        this.scalar = scalar;
    }

    @Override
    public Matrix apply(ComputationContext ctx) {

        double[] matrixData = ctx.data(matrix).data();
        double scalarValue = ctx.data(scalar).value();

        double[] result = new double[matrixData.length];

        for (int pos = 0; pos < matrixData.length; pos++) {
            result[pos] = matrixData[pos] + scalarValue;
        }

        return new Matrix(result, rows, cols);
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        if (parent == matrix) {
            return ctx.gradient(this);
        } else {
            var gradient = ctx.gradient(this);
            var result = 0d;
            for (int pos = 0; pos < gradient.totalSize(); pos++) {
                result += gradient.dataAt(pos);
            }

            return new Scalar(result);
        }
    }
}
