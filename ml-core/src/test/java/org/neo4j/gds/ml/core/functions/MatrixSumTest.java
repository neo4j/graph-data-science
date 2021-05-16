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
package org.neo4j.gds.ml.core.functions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MatrixSumTest {

    @Test
    void adds() {
        var operand = Constant.matrix(new double[]{1.0, 2.0, 3.0, 4.0}, 2, 2);
        MatrixSum add = new MatrixSum(List.of(operand, operand, operand));

        assertArrayEquals(new int[]{2, 2}, add.dimensions());

        ComputationContext ctx = new ComputationContext();

        var expected = new Matrix(new double[]{3.0, 6.0, 9.0, 12.0}, 2, 2);

        assertThat(ctx.forward(add)).isEqualTo(expected);
    }

    @Test
    void validatesDimensions() {
        var op1 = Constant.matrix(new double[]{1.0, 2.0, 3.0, 4.0}, 2, 2);
        var op2 = Constant.matrix(new double[]{1.0, 2.0, 3.0, 4.0}, 1, 4);

        assertThrows(AssertionError.class, () -> new MatrixSum(List.of(op1, op2, op1)));
    }

}
