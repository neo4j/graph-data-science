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

    public abstract SELF zeros();

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
        var result = zeros();
        Arrays.setAll(result.data, i -> f.applyAsDouble(data[i]));
        return result;
    }

    public void mapInPlace(DoubleUnaryOperator f) {
        Arrays.setAll(data, i -> f.applyAsDouble(data[i]));
    }

    // TODO: figure out how to replace this one
    public void addInPlace(Tensor<?> other) {
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

    public SELF scalarMultiply(double scalar) {
        SELF scaled = copy();
        scaled.scalarMultiplyMutate(scalar);
        return scaled;
    }

    public int totalSize() {
        return totalSize(dimensions);
    }

    // TODO: figure out how to replace this one
    public SELF elementwiseProduct(Tensor<?> other) {
        var result = zeros();
        for (int i = 0; i < data.length; i++) {
            result.data[i] = data[i] * other.data[i];
        }
        return result;
    }

    public double aggregateSum() {
        double sum = 0;
        for (double datum : data) {
            sum += datum;
        }
        return sum;
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

    public static long sizeInBytes(int[] dimensions) {
        return MemoryUsage.sizeOfDoubleArray(totalSize(dimensions));
    }
}
