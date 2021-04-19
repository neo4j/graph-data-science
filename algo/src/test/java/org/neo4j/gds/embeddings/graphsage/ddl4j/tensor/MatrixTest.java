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
package org.neo4j.gds.embeddings.graphsage.ddl4j.tensor;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ArrayUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;


class MatrixTest {

    @Test
    void returnsMatrixZeros() {
        Matrix matrix = Matrix.fill(6D, 3, 4);

        Matrix zeros = matrix.zeros();

        assertNotSame(matrix, zeros);
        assertArrayEquals(matrix.dimensions, zeros.dimensions);
        assertArrayEquals(ArrayUtil.fill(0D, 12), zeros.data);
    }

    @Test
    void createsMatrixFillCopy() {
        Matrix matrix = Matrix.fill(6D, 3, 4);

        Matrix copy = matrix.copy();

        assertNotSame(matrix, copy);
        assertArrayEquals(matrix.dimensions, copy.dimensions);
        assertArrayEquals(ArrayUtil.fill(6D, 12), copy.data);
    }

    @Test
    void createsMatrixCopy() {
        Matrix matrix = new Matrix(new double[]{1D, .1, 4D, -5D}, 2, 2);

        Matrix copy = matrix.copy();

        assertNotSame(matrix, copy);
        assertArrayEquals(matrix.dimensions, copy.dimensions);
        assertNotSame(matrix.data, copy.data);
        assertArrayEquals(matrix.data, copy.data);
    }

    @Test
    void addsMatrix() {
        var matrix = new Matrix(new double[] { 1D, 2D }, 1, 2);
        var matrixToAdd = new Matrix(new double[] { 10D, 12D }, 1, 2);

        Matrix sum = matrix.add(matrixToAdd);

        assertNotSame(matrix, sum);
        assertNotSame(matrixToAdd, sum);

        assertArrayEquals(new double[]{ 11D, 14D}, sum.data);
        assertArrayEquals(matrix.dimensions, sum.dimensions);
        assertArrayEquals(matrixToAdd.dimensions, sum.dimensions);
    }

    @Test
    void addsMatrixDifferentDimensions() {
        var matrix = new Matrix(new double[] { 1D, 2D }, 1, 2);
        var matrixToAdd = new Matrix(new double[] { 10D, 12D }, 2, 1);

        assertThrows(ArithmeticException.class, () -> matrix.add(matrixToAdd));
    }

    @Test
    void shouldEstimateMemory() {
        var dimensions = GraphDimensions.of(0);
        var _05_10 = Matrix.memoryEstimation(5, 10)
            .estimate(dimensions, 1)
            .memoryUsage();
        var _10_05 = Matrix.memoryEstimation(10, 5)
            .estimate(dimensions, 1)
            .memoryUsage();
        assertThat(_05_10).isEqualTo(_10_05);
    }
}
