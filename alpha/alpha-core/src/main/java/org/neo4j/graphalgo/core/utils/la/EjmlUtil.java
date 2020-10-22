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
package org.neo4j.graphalgo.core.utils.la;

import org.ejml.MatrixDimensionException;
import org.ejml.data.DMatrix1Row;

import java.util.function.IntPredicate;

public class EjmlUtil {
    /**
     * modified version of MatrixMatrixMult_DDRM#multTransB with a simple predicate based mask
     */
    public static void multTransB(DMatrix1Row a , DMatrix1Row b , DMatrix1Row c, IntPredicate shouldBeComputed)
    {
        if( a == c || b == c )
            throw new IllegalArgumentException("Neither 'a' or 'b' can be the same matrix as 'c'");
        else if( a.numCols != b.numCols ) {
            throw new MatrixDimensionException("The 'a' and 'b' matrices do not have compatible dimensions");
        }
        c.reshape(a.numRows,b.numRows);

        int cIndex = 0;
        int aIndexStart = 0;

        for( int xA = 0; xA < a.numRows; xA++ ) {
            int end = aIndexStart + b.numCols;
            int indexB = 0;
                for (int xB = 0; xB < b.numRows; xB++) {
                    if(shouldBeComputed.test(cIndex)) {
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
}
