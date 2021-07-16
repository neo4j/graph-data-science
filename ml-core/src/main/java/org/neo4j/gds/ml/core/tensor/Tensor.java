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
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.util.Arrays;
import java.util.function.DoubleUnaryOperator;

public abstract class Tensor<SELF extends Tensor<SELF>> {
    protected final double[] data;
    protected final int[] dimensions;

    public Tensor(double[] data, int[] dimensions) {
        this.data = data;
        this.dimensions = dimensions;
    }

    @Override
    public String toString() {
        return shortDescription() + ": " + Arrays.toString(data);
    }

    protected abstract String shortDescription();

    public abstract SELF createWithSameDimensions();

    public abstract SELF copy();

    public abstract SELF add(SELF b);

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

    public SELF map(DoubleUnaryOperator f) {
        var result = createWithSameDimensions();
        Arrays.setAll(result.data, i -> f.applyAsDouble(data[i]));
        return result;
    }

    public Tensor<SELF> mapInPlace(DoubleUnaryOperator f) {
        Arrays.setAll(data, i -> f.applyAsDouble(data[i]));
        return this;
    }

    // TODO: figure out how to replace this one
    public void addInPlace(Tensor<?> other) {
        int totalSize = Dimensions.totalSize(dimensions);
        for (int pos = 0; pos < totalSize; pos++) {
            data[pos] += other.data[pos];
        }
    }

    public Tensor<SELF> scalarMultiplyMutate(double scalar) {
        int totalSize = totalSize();
        for (int pos = 0; pos < totalSize; pos++) {
            data[pos] *= scalar;
        }

        return this;
    }

    public SELF scalarMultiply(double scalar) {
        SELF scaled = copy();
        scaled.scalarMultiplyMutate(scalar);
        return scaled;
    }

    public int totalSize() {
        return Dimensions.totalSize(dimensions);
    }

    // TODO: figure out how to replace this one
    public SELF elementwiseProduct(Tensor<?> other) {
        var result = createWithSameDimensions();
        for (int i = 0; i < data.length; i++) {
            result.data[i] = data[i] * other.data[i];
        }
        return result;
    }

    public Tensor<SELF> elementwiseProductMutate(Tensor<?> other) {
        for (int i = 0; i < data.length; i++) {
            this.data[i] = data[i] * other.data[i];
        }

        return this;
    }

    public double aggregateSum() {
        double sum = 0;
        for (double datum : data) {
            sum += datum;
        }
        return sum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return equals((Tensor<?>) o, 1e-32);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(data) + Arrays.hashCode(dimensions);
    }

    public static long sizeInBytes(int[] dimensions) {
        return MemoryUsage.sizeOfDoubleArray(Dimensions.totalSize(dimensions));
    }

    public boolean equals(Tensor<?> other, double tolerance) {
        if (!Arrays.equals(dimensions, other.dimensions)) {
            return false;
        }

        for (int i = 0; i < other.data.length; i++) {
            if (Math.abs(data[i] - other.data[i]) > tolerance) {
                return false;
            }
        }
        return true;
    }
}
