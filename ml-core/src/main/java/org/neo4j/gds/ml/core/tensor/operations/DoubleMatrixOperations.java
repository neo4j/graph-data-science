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
import org.ejml.data.DMatrix1Row;

import java.util.function.IntPredicate;

public final class DoubleMatrixOperations {

    /**
     * Modified version of Ejml implementation.
     *
     * @see <a href="https://github.com/lessthanoptimal/ejml/blob/v0.39/main/ejml-ddense/src/org/ejml/dense/row/mult/MatrixMatrixMult_DDRM.java#L317">MatrixMatrixMult_DDRM#multTransB</a>
     */
    public static void multTransB(DMatrix1Row a, DMatrix1Row b, DMatrix1Row c, IntPredicate mask) {
        if (a == c || b == c)
            throw new IllegalArgumentException("Neither 'a' or 'b' can be the same matrix as 'c'");
        else if (a.numCols != b.numCols) {
            throw new MatrixDimensionException("The 'a' and 'b' matrices do not have compatible dimensions");
        }
        c.reshape(a.numRows, b.numRows);

        int aIndexStart = 0;
        int cIndex = 0;

        for (int xA = 0; xA < a.numRows; xA++) {
            int end = aIndexStart + b.numCols;
            int indexB = 0;
            for (int xB = 0; xB < b.numRows; xB++) {
                if (mask.test(cIndex)) {
                    int indexA = aIndexStart;
                    double total = 0;

                    while (indexA < end) {
                        total += a.get(indexA++) * b.get(indexB++);
                    }

                    c.set(cIndex, total);
                } else {
                    indexB += b.numCols;
                }
                cIndex++;
            }
            aIndexStart += a.numCols;
        }
    }

    private DoubleMatrixOperations() {}
}
