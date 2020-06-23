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
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;

import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public abstract class MatrixVectorMultiply extends Variable {
    private final Variable matrix;
    private final Variable vector;

    public MatrixVectorMultiply(Variable matrix, Variable vector) {
        super(List.of(matrix, vector), Dimensions.vector(matrix.dimension(0)));

        checkDimensions(matrix, vector);

        this.matrix = matrix;
        this.vector = vector;
    }

    @Override
    protected Tensor apply(ComputationContext ctx) {
        Tensor weights = ctx.data(matrix);
        Tensor tensorVector = ctx.data(vector);

        double[] result = multiply(weights, tensorVector, false);

        return Tensor.vector(result);
    }

    @Override
    protected Tensor gradient(Variable parent, ComputationContext ctx) {
        if (parent == matrix) {
            // grad *outerproduct vector
            return outerProduct(ctx.gradient(this), ctx.data(vector));
        } else if (parent == vector) {
            // matrix *_mat multiply.grad
            return Tensor.vector(multiply(ctx.data(matrix), ctx.gradient(this), true));
        } else {
            throw new IllegalArgumentException("Variable requesting gradient does not match any of the parents.");
        }
    }

    protected abstract double[] multiply(Tensor matrix, Tensor vector, boolean transposeMatrix);

    private Tensor outerProduct(Tensor vector1, Tensor vector2) {
        int d1 = vector1.dimensions[0];
        int d2 = vector2.dimensions[0];
        Tensor result = Tensor.constant(0, Dimensions.matrix(d1, d2));

        for (int i = 0; i < d1; i++) {
            for (int j = 0; j < d2; j++) {
                result.set(vector1.getAtIndex(i) * vector2.getAtIndex(j), i, j);
            }
        }
        return result;
    }

    private void checkDimensions(Variable matrix, Variable vector) {
        int matrixColumns = matrix.dimension(1);
        int vectorSize = vector.dimension(0);
        if (matrixColumns != vectorSize) {
            throw new IllegalArgumentException(formatWithLocale(
                "Cannot multiply matrix having %d columns with a vector of size %d",
                matrixColumns,
                vectorSize
            ));
        }
    }

}
