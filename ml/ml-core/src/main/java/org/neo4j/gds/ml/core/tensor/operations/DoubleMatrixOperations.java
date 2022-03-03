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
package org.neo4j.gds.ml.core.tensor.operations;

import org.ejml.MatrixDimensionException;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.function.IntPredicate;

public final class DoubleMatrixOperations {

    /**
     * Modified version of Ejml implementation.
     *
     * @see <a href="https://github.com/lessthanoptimal/ejml/blob/v0.39/main/ejml-ddense/src/org/ejml/dense/row/mult/MatrixMatrixMult_DDRM.java#L317">MatrixMatrixMult_DDRM#multTransB</a>
     */
    public static void multTransB(Matrix a, Matrix b, Matrix c, IntPredicate mask) {
        if (a == c || b == c) {
            throw new IllegalArgumentException("Neither 'a' or 'b' can be the same matrix as 'c'");
        }

        int rowsA = a.rows();
        int colsA = a.cols();
        int rowsB = b.rows();
        int colsB = b.cols();

        if (colsA != colsB) {
            throw new MatrixDimensionException("The 'a' and 'b' matrices do not have compatible dimensions");
        }


        if (c.rows() != rowsA || c.cols() != rowsB) {
            throw new MatrixDimensionException("The matrix 'c` does not have compatible dimensions.");
        }

        int aIndexStart = 0;
        int cIndex = 0;

        for (int xA = 0; xA < rowsA; xA++) {
            int end = aIndexStart + colsB;
            int indexB = 0;
            for (int xB = 0; xB < rowsB; xB++) {
                if (mask.test(cIndex)) {
                    int indexA = aIndexStart;
                    double total = 0;

                    while (indexA < end) {
                        total += a.dataAt(indexA++) * b.dataAt(indexB++);
                    }

                    c.setDataAt(cIndex, total);
                } else {
                    indexB += colsB;
                }
                cIndex++;
            }
            aIndexStart += colsA;
        }
    }

    private DoubleMatrixOperations() {}
}
