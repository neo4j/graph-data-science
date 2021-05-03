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

import org.neo4j.gds.embeddings.graphsage.ddl4j.AbstractVariable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;

import java.util.List;

public class MatrixConstant extends AbstractVariable<Matrix> {

    private final Matrix data;

    public MatrixConstant(double[] elements, int rows, int cols) {
        super(List.of(), Dimensions.matrix(rows, cols));
        this.data = new Matrix(elements, rows, cols);
    }

    public static long sizeInBytes(int rows, int columns) {
        return Matrix.sizeInBytes(rows, columns);
    }

    @Override
    public Matrix apply(ComputationContext ctx) {
        return data;
    }

    @Override
    public Matrix gradient(Variable<?> parent, ComputationContext ctx) {
        return data.zeros();
    }
}
