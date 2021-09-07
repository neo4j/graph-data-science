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

/**
 * Corresponds to: result[i, j] = matrix[i, j] + scalar
 */
public class EWiseAddMatrixScalar extends AbstractVariable<Matrix> {

    private final Variable<Matrix> matrixVariable;
    private final Variable<Scalar> scalarVariable;

    public EWiseAddMatrixScalar(Variable<Matrix> matrixVariable, Variable<Scalar> scalarVariable) {
        super(List.of(matrixVariable, scalarVariable), matrixVariable.dimensions());
        this.matrixVariable = matrixVariable;
        this.scalarVariable = scalarVariable;
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        var matrix = ctx.data(matrixVariable);
        double scalarValue = ctx.data(scalarVariable).value();

        return matrix.map(v -> v + scalarValue);
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        if (parent == matrixVariable) {
            return ctx.gradient(this);
        } else {
            return new Scalar(ctx.gradient(this).aggregateSum());
        }
    }
}
