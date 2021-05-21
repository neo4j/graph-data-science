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
package org.neo4j.gds.ml.core.tensor;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.ArrayUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class MatrixTest {

    @Test
    void returnsMatrixZeros() {
        Matrix matrix = Matrix.fill(6D, 3, 4);

        Matrix zeros = matrix.zeros();
        var expected = new Matrix(ArrayUtil.fill(0D, 12), 3, 4);

        assertThat(zeros)
            .isNotSameAs(matrix)
            .isEqualTo(expected);
    }

    @Test
    void createsMatrixFillCopy() {
        Matrix matrix = Matrix.fill(6D, 3, 4);

        Matrix copy = matrix.copy();

        assertThat(copy)
            .isNotSameAs(matrix)
            .isEqualTo(matrix);
    }

    @Test
    void createsMatrixCopy() {
        Matrix matrix = new Matrix(new double[]{1D, .1, 4D, -5D}, 2, 2);

        Matrix copy = matrix.copy();

        assertThat(copy)
            .isNotSameAs(matrix)
            .isEqualTo(matrix);

        // test internal objects was also copied
        copy.setDataAt(2, 42);
        assertThat(copy).isNotEqualTo(matrix);
    }

    @Test
    void addsMatrix() {
        var matrix = new Matrix(new double[] { 1D, 2D }, 1, 2);
        var matrixToAdd = new Matrix(new double[] { 10D, 12D }, 1, 2);

        Matrix sum = matrix.add(matrixToAdd);
        var expected = new Matrix(new double[]{11D, 14D}, 1, 2);

        assertThat(sum)
            .isNotSameAs(matrix)
            .isNotSameAs(matrixToAdd)
            .isEqualTo(expected);
    }

    @Test
    void failOnAddingMatricesWithDifferentDimensions() {
        var matrix = new Matrix(new double[] { 1D, 2D }, 1, 2);
        var matrixToAdd = new Matrix(new double[] { 10D, 12D }, 2, 1);

        assertThatThrownBy(() -> matrix.add(matrixToAdd))
            .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void isVector() {
        assertThat(new Matrix(1, 3).isVector()).isTrue();
        assertThat(new Matrix(3, 1).isVector()).isTrue();
        assertThat(new Matrix(3, 3).isVector()).isFalse();
    }


    @Test
    void shouldEstimateMemory() {
        var _05_10 = Matrix.sizeInBytes(5, 10);
        var _10_05 = Matrix.sizeInBytes(10, 5);
        assertThat(_05_10).isEqualTo(_10_05);
    }

    @Test
    void testDataAtRowCol() {
        Matrix matrix = new Matrix(new double[]{
            1.5D, .1, 1.9,
            4D, -5D, 0
        }, 2, 3);


        assertThat(matrix.dataAt(0, 0)).isEqualTo(1.5);
        assertThat(matrix.dataAt(0, 1)).isEqualTo(.1);
        assertThat(matrix.dataAt(0, 2)).isEqualTo(1.9);
        assertThat(matrix.dataAt(1, 0)).isEqualTo(4);
        assertThat(matrix.dataAt(1, 1)).isEqualTo(-5);
        assertThat(matrix.dataAt(1, 2)).isEqualTo(0);

    }

    @Test
    void testSetDataAtRowCol() {
        Matrix matrix = Matrix.fill(0, 2, 3);

        matrix.setDataAt(0, 0, 1.5);
        matrix.setDataAt(0, 1, .1);
        matrix.setDataAt(0, 2, 1.9);
        matrix.setDataAt(1, 0, 4);
        matrix.setDataAt(1, 1, -5);
        matrix.setDataAt(1, 2, 0);

        assertThat(matrix.dataAt(0, 0)).isEqualTo(1.5);
        assertThat(matrix.dataAt(0, 1)).isEqualTo(.1);
        assertThat(matrix.dataAt(0, 2)).isEqualTo(1.9);
        assertThat(matrix.dataAt(1, 0)).isEqualTo(4);
        assertThat(matrix.dataAt(1, 1)).isEqualTo(-5);
        assertThat(matrix.dataAt(1, 2)).isEqualTo(0);
    }

    @Test
    void testAddDataAtRowCol() {
        Matrix matrix = Matrix.fill(1, 2, 3);

        matrix.addDataAt(0, 0, 1.5);
        matrix.addDataAt(0, 1, .1);
        matrix.addDataAt(0, 2, 1.9);
        matrix.addDataAt(1, 0, 4);
        matrix.addDataAt(1, 1, -5);
        matrix.addDataAt(1, 2, 0);

        assertThat(matrix.dataAt(0, 0)).isEqualTo(1 + 1.5);
        assertThat(matrix.dataAt(0, 1)).isEqualTo(1 + .1);
        assertThat(matrix.dataAt(0, 2)).isEqualTo(1 + 1.9);
        assertThat(matrix.dataAt(1, 0)).isEqualTo(1 + 4);
        assertThat(matrix.dataAt(1, 1)).isEqualTo(1 + -5);
        assertThat(matrix.dataAt(1, 2)).isEqualTo(1 + 0);
    }
}
