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

import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.graphalgo.core.utils.ArrayUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import static org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions.ROWS_INDEX;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class Matrix extends Tensor<Matrix> {

    public static MemoryEstimation memoryEstimation(int rows, int cols) {
        return MemoryEstimations.builder(Matrix.class)
            .fixed("data", MemoryUsage.sizeOfDoubleArray(rows * cols))
            .build();
    }

    public Matrix(double[] data, int rows, int cols) {
        super(data, Dimensions.matrix(rows, cols));
    }

    public Matrix(int rows, int cols) {
        this(new double[rows * cols], rows, cols);
    }

    public static Matrix fill(double v, int rows, int cols) {
        return new Matrix(ArrayUtil.fill(v, rows * cols), rows, cols);
    }

    @Override
    public Matrix zeros() {
        return fill(0D, rows(), cols());
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
        var sum = zeros();
        double[] localData = this.data;
        for (int i = 0; i < localData.length; ++i) {
            sum.data[i] = localData[i] + b.data[i];
        }
        return sum;
    }

    public int rows() {
        return dimensions[ROWS_INDEX];
    }

    public int cols() {
        return dimensions[COLUMNS_INDEX];
    }
}
