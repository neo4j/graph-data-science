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

import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.mult.MatrixMatrixMult_DDRM;
import org.neo4j.gds.collections.ArrayUtil;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.core.Dimensions;

import java.util.Arrays;
import java.util.function.DoubleUnaryOperator;

import static org.neo4j.gds.ml.core.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.ml.core.Dimensions.ROWS_INDEX;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Matrix extends Tensor<Matrix> {

    // FIXME: Adjust memory estimation because we added these two fields
    private final int columns;
    private final int rows;

    public static long sizeInBytes(int rows, int cols) {
        return MemoryUsage.sizeOfDoubleArray((long) rows * cols);
    }

    public Matrix(double[] data, int rows, int cols) {
        super(data, Dimensions.matrix(rows, cols));
        this.rows = rows;
        this.columns = cols;
    }

    public static Matrix of(DMatrixRMaj matrix) {
        return  new Matrix(matrix.data, matrix.numRows, matrix.numCols);
    }

    public Matrix(int rows, int cols) {
        this(new double[Math.multiplyExact(rows, cols)], rows, cols);
    }

    public static Matrix create(double v, int rows, int cols) {
        return new Matrix(ArrayUtil.fill(v, rows * cols), rows, cols);
    }

    public double dataAt(int row, int col) {
        return dataAt(row * columns + col);
    }

    public void setDataAt(int row, int column, double newValue) {
        setDataAt(row * columns + column, newValue);
    }

    public void setRow(int row, double[] values) {
        if (values.length != columns) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "Input vector dimension is unequal to column count of the matrix. Got %d, but expected %d.",
                    columns,
                    values.length
                ));
        }

        System.arraycopy(values, 0, data, row * columns, columns);
    }


    public double[] getRow(int rowIdx) {
        return Arrays.copyOfRange(data, rowIdx * columns, (rowIdx + 1) * columns);
    }

    public void addDataAt(int row, int column, double newValue) {
        updateDataAt(row, column, v -> v + newValue);
    }

    public void updateDataAt(int row, int column, DoubleUnaryOperator updater) {
        setDataAt(row, column, updater.applyAsDouble(dataAt(row, column)));
    }

    @Override
    public Matrix createWithSameDimensions() {
        return new Matrix(rows(), cols());
    }

    @Override
    public Matrix copy() {
        return new Matrix(data.clone(), rows(), cols());
    }

    @Override
    public Matrix add(Matrix b) {
        if (rows() != b.rows() || cols() != b.cols()) {
            throw new ArithmeticException(formatWithLocale(
                "Matrix dimensions must match! Got dimensions (%d, %d) + (%d, %d)",
                rows(),
                cols(),
                b.rows(),
                b.cols()
            ));
        }
        var sum = createWithSameDimensions();
        double[] localData = this.data;
        for (int i = 0; i < localData.length; ++i) {
            sum.data[i] = localData[i] + b.data[i];
        }
        return sum;
    }

    public Matrix multiply(Matrix other) {
        DMatrixRMaj result = new DMatrixRMaj(this.rows, other.cols());
        MatrixMatrixMult_DDRM.mult_reorder(this.toEjml(), other.toEjml(), result);
        return Matrix.of(result);
    }

    public Matrix multiplyTransB(Matrix other) {
        DMatrixRMaj result = new DMatrixRMaj(this.rows, other.rows);
        MatrixMatrixMult_DDRM.multTransB(this.toEjml(), other.toEjml(), result);
        return Matrix.of(result);
    }

    public Matrix multiplyTransA(Matrix other) {
        DMatrixRMaj prod = new DMatrixRMaj(this.cols(), other.cols());
        MatrixMatrixMult_DDRM.multTransA_reorder(this.toEjml(), other.toEjml(), prod);
        return Matrix.of(prod);
    }

    /**
     * C[a, b] = A[a, b] + v[b]
     */
    public Matrix sumBroadcastColumnWise(Vector vector) {
        var result = this.createWithSameDimensions();

        for(int row = 0; row < rows; row++) {
            for (int col = 0; col < columns; col++) {
                int matrixIndex = row * columns + col;
                result.data[matrixIndex] = data[matrixIndex] + vector.data[col];
            }
        }

        return result;
    }

    public Vector sumPerColumn() {
        double[] result = new double[columns];

        for (int col = 0; col < columns; col++) {
            for (int row = 0; row < rows; row++) {
                result[col] += data[row * columns + col];
            }
        }

        return new Vector(result);
    }

    public void setRow(int rowIdx, Matrix input, int inputRowIdx) {
        if (input.columns != this.columns) {
            throw new ArithmeticException(formatWithLocale(
                "Input matrix must have the same number of columns. Expected %s, but got %s.",
                this.columns, input.columns
            )
            );
        }

        System.arraycopy(input.data, inputRowIdx * input.columns, this.data, rowIdx * columns, input.columns);
    }

    @Override
    public String shortDescription() {
        return formatWithLocale("Matrix(%d, %d)", rows(), cols());
    }

    public int rows() {
        return rows;
    }

    public int cols() {
        return columns;
    }

    public boolean isVector() {
        return Dimensions.isVector(dimensions);
    }

    public DMatrixRMaj toEjml() {
        return DMatrixRMaj.wrap(this.dimension(ROWS_INDEX), this.dimension(COLUMNS_INDEX), this.data());
    }
}
