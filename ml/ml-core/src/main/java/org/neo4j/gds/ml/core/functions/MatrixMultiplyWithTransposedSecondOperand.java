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
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.List;

import static org.neo4j.gds.ml.core.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.ml.core.Dimensions.ROWS_INDEX;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MatrixMultiplyWithTransposedSecondOperand extends AbstractVariable<Matrix> {

    public static long sizeInBytes(int leftMatrixNumRows, int rightMatrixNumRows) {
        return Matrix.sizeInBytes(leftMatrixNumRows, rightMatrixNumRows);
    }

    private final Variable<Matrix> A;
    private final Variable<Matrix> B;

    public MatrixMultiplyWithTransposedSecondOperand(Variable<Matrix> A, Variable<Matrix> B) {
        super(List.of(A, B), Dimensions.matrix(A.dimension(ROWS_INDEX), B.dimension(ROWS_INDEX)));
        // The dimensions of a matrix multiplication of dimensions (m, n) x (n, p) = (m, p)
        // When B is of the dimensions (p, n) it needs to be transposed in order to allow the multiplication.
        // When B is being transposed as B_T its dimensions become (n, p)
        assertDimensions(A, B);

        this.A = A;
        this.B = B;
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        return ctx.data(A).multiplyTransB(ctx.data(B));
    }

    @Override
    public Matrix gradient(Variable<?> parent, ComputationContext ctx) {
        var gradient = ctx.gradient(this);
        if (parent == A) {
            return gradient.multiply(ctx.data(B));
        } else {
            return gradient.multiplyTransA(ctx.data(A));
        }
    }

    public static MatrixMultiplyWithTransposedSecondOperand of(Variable<Matrix> A, Variable<Matrix> B) {
        return new MatrixMultiplyWithTransposedSecondOperand(A, B);
    }

    private void assertDimensions(Variable<Matrix> A, Variable<Matrix> B) {
        assert A.dimension(COLUMNS_INDEX) == B.dimension(COLUMNS_INDEX) : formatWithLocale(
            "Cannot multiply matrix having dimensions (%d, %d) with transposed matrix of dimensions (%d, %d)",
            A.dimension(COLUMNS_INDEX), A.dimension(ROWS_INDEX),
            B.dimension(ROWS_INDEX), B.dimension(COLUMNS_INDEX)
        );
    }
}
