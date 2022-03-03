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

public class Slice extends SingleParentVariable<Matrix, Matrix> {

    private final int[] batchIds;

    public Slice(Variable<Matrix> parent, int[] batchIds) {
        super(parent, Dimensions.matrix(batchIds.length, parent.dimension(Dimensions.COLUMNS_INDEX)));

        this.batchIds = batchIds;
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        Matrix parentData = ctx.data(parent);
        var rows = batchIds.length;

        Matrix result = new Matrix(rows, parentData.cols());

        for (int row = 0; row < rows; row++) {
            result.setRow(row, parentData, batchIds[row]);
        }

        return result;
    }

    @Override
    public Matrix gradientForParent(ComputationContext ctx) {
        Matrix thisGradient = ctx.gradient(this);

        Matrix result = ctx.data(parent).createWithSameDimensions();
        var rows = batchIds.length;
        var cols = thisGradient.cols();

        for (int row = 0; row < rows; row++) {
            int childRow = batchIds[row];
            for (int col = 0; col < cols; col++) {
                result.addDataAt(childRow, col, thisGradient.dataAt(row, col));
            }
        }

        return result;
    }
}
