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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class Vector {
    private final double[] values;

    Vector(double[] values) {
        this.values = values;
    }

    double innerProduct(Vector other) {
        double result = 0;
        for (int position = 0; position < values.length; position++) {
            result += values[position] * other.values[position];
        }
        return result;
    }

    private Vector copy() {
        return new Vector(Arrays.copyOf(values, values.length));
    }

    Vector multiply(double scalar) {
        Vector newVector = copy();
        newVector.multiplyInPlace(scalar);
        return newVector;
    }

    void multiplyInPlace(double scalar) {
        for (int position = 0; position < values.length; position++) {
            values[position] *= scalar;
        }
    }

    static Vector zero(int dimension) {
        return new Vector(new double[dimension]);
    }

    Vector add(Vector other) {
        double[] newValues = new double[values.length];
        for (int position = 0; position < values.length; position++) {
                newValues[position] = values[position] + other.values[position];
        }
        return new Vector(newValues);
    }

    Vector subtract(Vector other) {
        Vector result = copy();
        for (int position = 0; position < values.length; position++) {
            result.values[position] -= other.values[position];
        }
        return result;
    }

    void addInPlace(Vector other) {
        for (int position = 0; position < values.length; position++) {
            values[position] += other.values[position];
        }
    }

    void subtractInPlace(Vector other) {
        for (int position = 0; position < values.length; position++) {
            values[position] -= other.values[position];
        }
    }

    // we only support sum of empty collection if dimension is 0
    static Vector sum(Collection<Vector> vectors) {
        if (vectors.isEmpty()) {
            return zero(0);
        }
        int dim = vectors.stream().findFirst().get().values.length;
        Vector result = zero(dim);
        for (Vector vector : vectors) {
            result.addInPlace(vector);
        }
        return result;
    }

    double l2() {
        return innerProduct(this);
    }

    double l1() {
        double result = 0;
        for (double val : values) {
            result += Math.abs(val);
        }
        return result;
    }

    int dim() {
        return values.length;
    }

    public List<Integer> exceeding(double delta) {
        List<Integer> exceedingIndices = new LinkedList<>();
        for (int position = 0; position < values.length; position++) {
            if (values[position] > delta) {
                exceedingIndices.add(position);
            }
        }
        return exceedingIndices;
    }

    public List<Double> exceedingScores(double delta) {
        List<Double> scores = new LinkedList<>();
        for (double value : values) {
            if (value > delta) {
                scores.add(value);
            }
        }
        return scores;
    }

    Vector addAndProject(Vector increment) {
        Vector newVector = add(increment);
        for (int pos = 0; pos < newVector.dim(); pos++) {
            if (newVector.values[pos] < 0) {
                newVector.values[pos] = 0;
            }
        }
        return newVector;
    }

    static Vector l1PenaltyGradient(int dimension, double lambda) {
        double[] newValues = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            newValues[i] = lambda;
        }
        return new Vector(newValues);
    }
}
