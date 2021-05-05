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
import org.neo4j.gds.ml.core.tensor.Tensor;

import java.util.Arrays;
import java.util.List;

import static org.neo4j.gds.ml.core.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.ml.core.Dimensions.ROWS_INDEX;

public class MatrixSum extends AbstractVariable<Matrix> {

    public MatrixSum(List<Variable<Matrix>> parents) {
        super(parents, validatedDimensions(parents));
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        Matrix sum = Matrix.fill(0D, dimension(ROWS_INDEX), dimension(COLUMNS_INDEX));
        for (Variable<?> parent : parents()) {
            sum.addInPlace(ctx.data(parent));
        }
        return sum;
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        return ctx.gradient(this);
    }

    private static int[] validatedDimensions(List<Variable<Matrix>> parents) {
        int[] dimensions = parents.get(0).dimensions();
        parents.forEach(v -> {
            assert Arrays.equals(v.dimensions(), dimensions);
        });
        return dimensions;
    }
}
