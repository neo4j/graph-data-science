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
import org.neo4j.gds.collections.ArrayUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class MatrixTest {

    @Test
    void returnsMatrixZeros() {
        Matrix matrix = Matrix.create(6D, 3, 4);

        Matrix zeros = matrix.createWithSameDimensions();
        var expected = new Matrix(ArrayUtil.fill(0D, 12), 3, 4);

        assertThat(zeros)
            .isNotSameAs(matrix)
            .isEqualTo(expected);
    }

    @Test
    void createsMatrixFillCopy() {
        Matrix matrix = Matrix.create(6D, 3, 4);

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
        Matrix matrix = Matrix.create(0, 2, 3);

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
        Matrix matrix = Matrix.create(1, 2, 3);

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

    @Test
    void testUpdateDataAtRowCol() {
        Matrix matrix = Matrix.create(2, 2, 3);

        matrix.updateDataAt(0, 0, v -> v * v);
        matrix.updateDataAt(0, 1, v -> v - 1);
        matrix.updateDataAt(0, 2, v -> v / 4);
        matrix.updateDataAt(1, 0, v -> v + 1);
        matrix.updateDataAt(1, 1, v -> v);
        matrix.updateDataAt(1, 2, v -> v * 5);

        assertThat(matrix.dataAt(0, 0)).isEqualTo(4);
        assertThat(matrix.dataAt(0, 1)).isEqualTo(1);
        assertThat(matrix.dataAt(0, 2)).isEqualTo(0.5);
        assertThat(matrix.dataAt(1, 0)).isEqualTo(3);
        assertThat(matrix.dataAt(1, 1)).isEqualTo(2);
        assertThat(matrix.dataAt(1, 2)).isEqualTo(10);
    }

    @Test
    void setRow() {
        var matrix = Matrix.create(0, 3, 2);

        matrix.setRow(1, new double[] {42, 99});

        assertThat(matrix).isEqualTo(new Matrix(new double[] {0,0, 42, 99, 0,0}, 3, 2));
    }

    @Test
    void getRow() {
        var matrix = new Matrix(new double[] {1, 2, 3, 4, 5, 6}, 3, 2);

        assertThat(matrix.getRow(0)).containsExactly(1, 2);
        assertThat(matrix.getRow(1)).containsExactly(3, 4);
        assertThat(matrix.getRow(2)).containsExactly(5, 6);
    }

    @Test
    void sumPerColumn() {
        var matrix = new Matrix(new double[] {
            1, 2, 3,
            4, 5, 6
        }, 2, 3);

        assertThat(matrix.sumPerColumn()).isEqualTo(new Vector(5, 7, 9));
    }

    @Test
    void multiply() {
        var matrix =  new Matrix(new double[] {1, 2, 3, 4, 5, 6}, 3, 2);
        var otherMatrix =  new Matrix(new double[] {1, 2, 3, 4, 5, 6}, 2, 3);

        assertThat(matrix.multiply(otherMatrix)).isEqualTo(new Matrix(new double[]{
            9.0, 12.0, 15.0,
            19.0, 26.0, 33.0,
            29.0, 40.0, 51.0
        }, 3, 3));
    }

    @Test
    void multiplyTransB() {
        var matrix =  new Matrix(new double[] {1, 2, 3, 4, 5, 6}, 3, 2);
        var otherMatrix =  new Matrix(new double[] {1, 2, 3, 4, 5, 6}, 3, 2);

        assertThat(matrix.multiplyTransB(otherMatrix)).isEqualTo(new Matrix(new double[]{
            5.0, 11.0, 17.0,
            11.0, 25.0, 39.0,
            17.0, 39.0, 61.0
        }, 3, 3));
    }

    @Test
    void multiplyTransA() {
        var matrix =  new Matrix(new double[] {1, 2, 3, 4, 5, 6}, 2, 3);
        var otherMatrix =  new Matrix(new double[] {1, 2, 3, 4, 5, 6}, 2, 3);

        assertThat(matrix.multiplyTransA(otherMatrix)).isEqualTo(new Matrix(new double[]{
            17.0, 22.0, 27.0,
            22.0, 29.0, 36.0,
            27.0, 36.0, 45.0
        }, 3, 3));
    }

    @Test
    void sumBroadcastColumnWise() {
        var matrix = new Matrix(new double[] {
            1, 2, 3,
            4, 5, 6
        }, 2, 3);

        var vector = new Vector(0.5, 2.5, 1.0);

        var expected = new Matrix(new double[] {
            1.5, 4.5, 4.0,
            4.5, 7.5, 7.0
        }, 2, 3);

        assertThat(matrix.sumBroadcastColumnWise(vector)).isEqualTo(expected);
    }

    @Test
    void setRowFromMatrix() {
        var matrix = new Matrix(new double[] {
            1, 2, 3,
            4, 5, 6
        }, 2, 3);

        var result = new Matrix(3, 3);

        result.setRow(0, matrix, 1);
        result.setRow(2, matrix, 0);

        assertThat(result).isEqualTo(new Matrix(new double[] { 4, 5, 6, 0,0,0, 1,2,3}, 3, 3));
    }

    @Test
    void failSetRowFromDifferentSizedMatrix() {
        var matrix = new Matrix(new double[] {1, 2, 4, 5}, 2, 2);

        var otherMatrix = new Matrix(new double[] {
            6, 7, 8,
            9, 10, 11,
            12, 13, 14
        }, 3, 3);

        assertThatThrownBy(() -> otherMatrix.setRow(1, matrix, 0))
            .hasMessage("Input matrix must have the same number of columns. Expected 3, but got 2.");

        assertThatThrownBy(() -> matrix.setRow(2, otherMatrix, 0))
            .hasMessage("Input matrix must have the same number of columns. Expected 2, but got 3.");
    }

    @Test
    void failSettingInvalidRow() {
        var matrix = Matrix.create(0, 3, 2);
        assertThatThrownBy(() -> matrix.setRow(1, new double[]{42}))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Input vector dimension is unequal to column count of the matrix. Got 2, but expected 1.");
    }

    @Test
    void testToString() {
        Matrix matrix = new Matrix(new double[]{1, 2, 3, 4}, 2, 2);

        assertThat(matrix.shortDescription()).isEqualTo("Matrix(2, 2)");
        assertThat(matrix.toString()).isEqualTo("Matrix(2, 2): [1.0, 2.0, 3.0, 4.0]");
    }
}
