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

import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.mult.MatrixMatrixMult_DDRM;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.IntPredicate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DoubleMatrixOperationsTest {

    @Test
    void multTransBWithMask() {
        int size = 1000;

        var raw = new double[size * size];
        Arrays.fill(raw, 42);

        var matrix = DMatrixRMaj.wrap(size, size, raw);
        var maskedResult = new DMatrixRMaj(size, size);
        IntPredicate mask = index -> index < size / 2;
        DoubleMatrixOperations.multTransB(matrix, matrix, maskedResult, mask);

        var originalResult = maskedResult.createLike();
        MatrixMatrixMult_DDRM.multTransB(matrix, matrix, originalResult);

        for (int index = 0; index < originalResult.data.length; index++) {
            if (mask.test(index)) {
                assertEquals(originalResult.get(index), maskedResult.get(index));
            } else {
                assertEquals(0, maskedResult.get(index));
            }
        }
    }

}
