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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RenderTest {

    @Test
    void renderMultiLevel() {
        var matrix = new MatrixConstant(new double[]{0, 1, 0, 1}, 2, 2);
        var otherMatrix = new MatrixConstant(new double[]{1, 0, 1, 0}, 2, 2);

        var elementSum = new MatrixSum(List.of(matrix, otherMatrix));
        var sigmoid = new Sigmoid<>(elementSum);

        assertThat(sigmoid.render()).isEqualTo(
            "Sigmoid: Matrix(2, 2)" + System.lineSeparator() +
            "|-- MatrixSum: Matrix(2, 2)" + System.lineSeparator() +
            "\t|-- MatrixConstant: Matrix(2, 2)" + System.lineSeparator() +
            "\t|-- MatrixConstant: Matrix(2, 2)" + System.lineSeparator()
        );
    }
}
