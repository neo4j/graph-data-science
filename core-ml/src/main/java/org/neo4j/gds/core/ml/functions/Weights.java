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
package org.neo4j.gds.core.ml.functions;

import org.neo4j.gds.core.ml.AbstractVariable;
import org.neo4j.gds.core.ml.ComputationContext;
import org.neo4j.gds.core.ml.Variable;
import org.neo4j.gds.core.ml.tensor.Matrix;
import org.neo4j.gds.core.ml.tensor.Tensor;

import java.util.List;

public class Weights<T extends Tensor<T>> extends AbstractVariable<T> {
    private final T data;

    public Weights(T data) {
        super(List.of(), data.dimensions());
        this.data = data;
    }

    @Override
    public T apply(ComputationContext ctx) {
        return data;
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        throw new NotAFunctionException();
    }

    public T data() {
        return data;
    }

    @Override
    public boolean requireGradient() {
        return true;
    }

    public static Weights<Matrix> ofMatrix(int rows, int cols) {
        return new Weights<>(new Matrix(rows, cols));
    }

    public static long sizeInBytes(int rows, int cols) {
        return Matrix.sizeInBytes(rows, cols);
    }

}
