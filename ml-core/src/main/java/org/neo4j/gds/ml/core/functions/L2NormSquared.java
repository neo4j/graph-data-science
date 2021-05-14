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
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;

public class L2NormSquared extends SingleParentVariable<Scalar> {

    public L2NormSquared(Variable<Matrix> parent) {
        super(parent, Dimensions.scalar());
    }

    public static long sizeInBytesOfApply() {
        return Scalar.sizeInBytes();
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        var parent = (Matrix) ctx.data(parent());
        var rows = parent.rows();
        var cols = parent.cols();

        var biasColumnIndex = cols - 1;
        double l2NormWithoutBias = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < biasColumnIndex; col++) {
                var value = parent.dataAt(row * cols + col);
                l2NormWithoutBias += (value * value);
            }
        }

        return new Scalar(l2NormWithoutBias);
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        var data = (Matrix) ctx.data(parent).copy();
        var rows = data.rows();
        var cols = data.cols();
        var biasColumnIndex = cols - 1;

        for (int row = 0; row < rows; row++) {
            data.setDataAt(row * cols + biasColumnIndex, 0);
        }

        return data.scalarMultiply(2 * ctx.gradient(this).value());
    }
}
