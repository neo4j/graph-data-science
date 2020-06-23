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

public abstract class MatrixMultiplyTransB extends Variable {
    private final Variable A;
    private final Variable B;

    protected MatrixMultiplyTransB(Variable A, Variable B) {
        super(List.of(A, B), Dimensions.matrix(A.dimension(0), B.dimension(0)));
        this.A = A;
        this.B = B;
    }

    @Override
    protected Tensor apply(ComputationContext ctx) {
        Tensor t1 = ctx.data(A);
        Tensor t2 = ctx.data(B);
        return multiplyTransB(t1, t2);
    }

    @Override
    protected Tensor gradient(Variable parent, ComputationContext ctx) {
        Tensor gradient = ctx.gradient(this);
        if(parent == A) {
            return multiply(gradient, ctx.data(B));
        } else {
            return multiplyTransA(gradient, ctx.data(A));
        }
    }

    protected abstract Tensor multiply(Tensor t1, Tensor t2);
    protected abstract Tensor multiplyTransB(Tensor t1, Tensor t2);
    protected abstract Tensor multiplyTransA(Tensor t1, Tensor t2);
}
