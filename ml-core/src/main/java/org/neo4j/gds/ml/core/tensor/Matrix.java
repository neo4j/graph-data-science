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

import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.graphalgo.core.utils.ArrayUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.util.function.DoubleUnaryOperator;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class Matrix extends Tensor<Matrix> {

    // FIXME: Adjust memory estimation because we added these two fields
    private final int columns;
    private final int rows;

    public static long sizeInBytes(int rows, int cols) {
        return MemoryUsage.sizeOfDoubleArray(rows * cols);
    }

    public Matrix(double[] data, int rows, int cols) {
        super(data, Dimensions.matrix(rows, cols));
        this.rows = rows;
        this.columns = cols;
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

    public void addDataAt(int row, int column, double newValue) {
        updateDataAt(row, column, v -> v + newValue);
    }

    public void updateDataAt(int row, int column, DoubleUnaryOperator updater) {
        setDataAt(row, column, updater.applyAsDouble(dataAt(row, column)));
    }

    @Override
    public Matrix createWithSameDimensions() {
        return create(0D, rows(), cols());
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
}
