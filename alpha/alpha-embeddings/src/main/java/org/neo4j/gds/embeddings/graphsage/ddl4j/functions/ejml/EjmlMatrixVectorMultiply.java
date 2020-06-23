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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions.ejml;

import org.ejml.data.DMatrixD1;
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.mult.MatrixVectorMult_DDRM;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixVectorMultiply;

public class EjmlMatrixVectorMultiply extends MatrixVectorMultiply {

    public EjmlMatrixVectorMultiply(Variable matrix, Variable vector) {
        super(matrix, vector);
    }

    protected double[] multiply(Tensor matrix, Tensor vector, boolean transposeMatrix) {
        DMatrixRMaj weights = DMatrixRMaj.wrap(matrix.dimensions[0], matrix.dimensions[1], matrix.data);
        DMatrixRMaj representationVector = new DMatrixRMaj(vector.data);
        DMatrixD1 multiplied = new DMatrixRMaj(matrix.dimensions[0], 1);
        if (transposeMatrix) {
            MatrixVectorMult_DDRM.multTransA_reorder(weights, representationVector, multiplied);
        } else {
            MatrixVectorMult_DDRM.mult(weights, representationVector, multiplied);
        }
        return multiplied.getData();
    }
}
