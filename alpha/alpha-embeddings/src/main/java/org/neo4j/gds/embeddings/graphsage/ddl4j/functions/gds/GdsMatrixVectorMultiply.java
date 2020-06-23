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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions.gds;

import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixVectorMultiply;

public class GdsMatrixVectorMultiply extends MatrixVectorMultiply {

    public GdsMatrixVectorMultiply(Variable matrix, Variable vector) {
        super(matrix, vector);
    }

    protected double[] multiply(Tensor matrix, Tensor vector, boolean transponse) {
        double[] result = new double[matrix.dimensions[0]];
        for (int i = 0; i < matrix.dimensions[0]; i++) {
            for (int j = 0; j < matrix.dimensions[1]; j++) {
                result[i] += matrix.get(i, j) * vector.get(j);
            }
        }
        return result;
    }

}
