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
package org.neo4j.graphalgo.impl.ocd;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.LinkedList;

public class SparseVector {
    private final int[] indices;
    private final double[] values;

    SparseVector(int[] indices, double[] values) {
        this.indices = indices;
        this.values = values;
    }

    @NotNull
    private static SparseVector getSparseVectorFromLists(LinkedList<Integer> indices, LinkedList<Double> values) {
        int[] indicesArray = new int[indices.size()];
        double[] valuesArray = new double[indices.size()];
        int i = 0;
        for (int index : indices) {
            indicesArray[i] = index;
            i++;
        }
        i = 0;
        for (double value : values) {
            valuesArray[i] = value;
            i++;
        }
        return new SparseVector(indicesArray, valuesArray);
    }


    public double innerProduct(SparseVector other) {
        int position = 0;
        int otherPosition = 0;
        int result = 0;
        while (position < dim() && otherPosition < other.dim()) {
            int index = indices[position];
            int otherIndex = other.indices[otherPosition];
            if (index == otherIndex) {
                result += values[position] * other.values[otherPosition];
                position++;
                otherPosition++;
            } else if (index < otherIndex) {
                position++;
            } else {
                otherPosition++;
            }
        }
        return result;
    }

    public SparseVector multiply(double scalar) {
        double[] newValues = new double[this.dim()];
        for (int i = 0; i < this.dim(); i++) {
            newValues[i] = scalar * values[i];
        }
        return new SparseVector(indices, newValues);
    }

    public SparseVector negate() {
        return multiply(-1D);
    }

    public SparseVector add(SparseVector other) {
        int position = 0;
        int otherPosition = 0;
        LinkedList<Integer> indices = new LinkedList<>();
        LinkedList<Double> values = new LinkedList<>();
        while (position < dim() && otherPosition < other.dim()) {
            int index = this.indices[position];
            int otherIndex = other.indices[otherPosition];
            if (index == otherIndex) {
                indices.add(index);
                values.add(this.values[position] + other.values[otherPosition]);
                position++;
                otherPosition++;
            } else if (index < otherIndex) {
                indices.add(index);
                values.add(this.values[position]);
                position++;
            } else {
                indices.add(otherIndex);
                values.add(other.values[position]);
                otherPosition++;
            }
        }
        return getSparseVectorFromLists(indices, values);
    }

    public static SparseVector sum(Collection<SparseVector> vectors) {
        int[] positions = new int[vectors.size()];
        LinkedList<Integer> indices = new LinkedList<>();
        LinkedList<Double> values = new LinkedList<>();
        int exhausted = 0;
        while (exhausted < vectors.size()) {
            int offset = 0;
            int minimumIndex = Integer.MAX_VALUE;
            double summedValue = 0;
            for (SparseVector vector : vectors) {
                int position = positions[offset];
                if (position < vector.dim()) {
                    int index = vector.indices[position];
                    if (index < minimumIndex) {
                        minimumIndex = index;
                    }
                }
                offset++;
            }
            offset = 0;
            for (SparseVector vector : vectors) {
                int position = positions[offset];
                if (position < vector.dim() && vector.indices[position] == minimumIndex) {
                    summedValue += vector.values[position];
                    positions[offset] += 1;
                    if (positions[offset] == vector.dim()) {
                        exhausted += 1;
                    }
                }
                offset++;
            }
            indices.add(minimumIndex);
            values.add(summedValue);
        }
        return getSparseVectorFromLists(indices, values);
    }

    public double l2() {
        return innerProduct(this);
    }

    public double l1() {
        double result = 0;
        for (double val : values) {
            result += Math.abs(val);
        }
        return result;
    }

    public int dim() {
        return indices.length;
    }

}
