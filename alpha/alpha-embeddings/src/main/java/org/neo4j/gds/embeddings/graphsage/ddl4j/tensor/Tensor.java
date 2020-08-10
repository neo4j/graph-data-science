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
package org.neo4j.gds.embeddings.graphsage.ddl4j.tensor;

import java.util.function.DoubleUnaryOperator;

public class Tensor {
    private final double[] data;
    private final int[] dimensions;

    public Tensor(double[] data, int[] dimensions) {
        this.data = data;
        this.dimensions = dimensions;
    }

    public int dimension(int dimensionIndex) {
        return dimensions[dimensionIndex];
    }

    public int[] dimensions() {
        return dimensions;
    }

    public double[] data() {
        return data;
    }

    public double dataAt(int idx) {
        return data[idx];
    }

    public void setDataAt(int idx, double newValue) {
        data[idx] = newValue;
    }

    public void addDataAt(int idx, double newValue) {
        data[idx] += newValue;
    }

    public Tensor zeros() {
        return new Tensor(new double[data.length], dimensions);
    }

    @Override
    public Tensor clone() {
        return new Tensor(data.clone(), dimensions.clone());
    }

    public int indexOf(int[] coordinates) {
        // TODO: Is `assert` enough?
        assert coordinates.length == dimensions.length : "coordinates should have the same size as dimensions: " + dimensions.length;

        int index = 0;
        int factor = 1;
        for (int dimension = coordinates.length - 1; dimension >= 0; dimension--) {
            index += coordinates[dimension] * factor;
            factor *= dimensions[dimension];
        }
        return index;
    }

    public double get(int... coordinates) {
        return data[indexOf(coordinates)];
    }

    // TODO: Check if we can use the varargs version instead.
    public double getAtIndex(int index) {
        return data[index];
    }

    public void set(double value, int... coordinates) {
        data[indexOf(coordinates)] = value;
    }

    public Tensor map(DoubleUnaryOperator f) {
        var result = zeros();
        for (int i = 0; i < data.length; i++) {
            result.data[i] = f.applyAsDouble(data[i]);
        }
        return result;
    }

    public void mapInPlace(DoubleUnaryOperator f) {
        for (int i = 0; i < data.length; i++) {
            data[i] = f.applyAsDouble(data[i]);
        }
    }

    public static Tensor add(Tensor a, Tensor b) {
        int totalSize = totalSize(a.dimensions);
        double[] sum = new double[totalSize];
        for (int pos = 0; pos < totalSize; pos++) {
            sum[pos] = a.data[pos] + b.data[pos];
        }
        return new Tensor(sum, a.dimensions);
    }

    public void addInPlace(Tensor other) {
        int totalSize = totalSize(dimensions);
        for (int pos = 0; pos < totalSize; pos++) {
            data[pos] += other.data[pos];
        }
    }

    public void scalarMultiplyMutate(double scalar) {
        int totalSize = totalSize();
        for (int pos = 0; pos < totalSize; pos++) {
            data[pos] *= scalar;
        }
    }

    public Tensor scalarMultiply(double scalar) {
        Tensor scaled = clone();
        scaled.scalarMultiplyMutate(scalar);
        return scaled;
    }

    public int totalSize() {
        return totalSize(dimensions);
    }

    public static int totalSize(int[] dimensions) {
        if (dimensions.length == 0) {
            return 0;
        }
        int totalSize = 1;
        for (int dim : dimensions) {
            totalSize *= dim;
        }
        return totalSize;
    }

    public double innerProduct(Tensor other) {
        double result = 0;
        for (int i = 0; i < data.length; i++) {
            result += data[i] * other.data[i];
        }
        return result;
    }

    public Tensor elementwiseProduct(Tensor other) {
        var result = zeros();
        for (int i = 0; i < data.length; i++) {
            result.data[i] = data[i] * other.data[i];
        }
        return result;
    }

    public double sum() {
        double sum = 0;
        for (double datum : data) {
            sum += datum;
        }
        return sum;
    }
}
