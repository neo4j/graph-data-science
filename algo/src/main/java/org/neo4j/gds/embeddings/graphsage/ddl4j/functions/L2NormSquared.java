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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;

public class L2NormSquared extends SingleParentVariable<Scalar> {

    public L2NormSquared(Variable<Matrix> parent) {
        super(parent, Dimensions.scalar());
    }

    public static long sizeInBytesOfApply() {
        return sizeOfDoubleArray(1);
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        Matrix p = (Matrix) ctx.data(parent());
        var rows = p.rows();
        var cols = p.cols();

        var biasColumnIndex = cols - 1;
        double l2NormWithoutBias = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < biasColumnIndex; col++) {
                var value = p.dataAt(row * cols + col);
                l2NormWithoutBias += (value * value);
            }
        }

        return new Scalar(l2NormWithoutBias);
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        Matrix data = (Matrix) ctx.data(parent).copy();
        var rows = data.rows();
        var cols = data.cols();
        var biasColumnIndex = cols - 1;

        for (int row = 0; row < rows; row++) {
            data.setDataAt(row * cols + biasColumnIndex, 0);
        }

        return data.scalarMultiply(2 * ctx.gradient(this).dataAt(0));
    }
}
