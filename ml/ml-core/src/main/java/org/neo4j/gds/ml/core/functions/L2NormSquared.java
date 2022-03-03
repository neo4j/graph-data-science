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

public class L2NormSquared extends SingleParentVariable<Matrix, Scalar> {

    public L2NormSquared(Variable<Matrix> parent) {
        super(parent, Dimensions.scalar());
    }

    public static long sizeInBytesOfApply() {
        return Scalar.sizeInBytes();
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        var parentMatrix = ctx.data(parent);
        var rows = parentMatrix.rows();
        var cols = parentMatrix.cols();

        double l2Norm = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                var value = parentMatrix.dataAt(row * cols + col);
                l2Norm += (value * value);
            }
        }

        return new Scalar(l2Norm);
    }

    @Override
    public Matrix gradientForParent(ComputationContext ctx) {
        return ctx.data(parent).copy().scalarMultiply(2 * ctx.gradient(this).value());
    }
}
