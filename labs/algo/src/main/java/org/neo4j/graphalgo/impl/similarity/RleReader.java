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
package org.neo4j.graphalgo.impl.similarity;

public class RleReader {

    private final double[] decodedVector;
    private double[] vector;

    private double value;

    private int index = 0;
    private int count;

    public RleReader(int decodedVectorSize) {
        this.decodedVector = new double[decodedVectorSize];
    }

    public void reset(double[] vector) {
        if (this.vector == null || !this.vector.equals(vector)) {
            this.vector = vector;
            reset();
            compute();
        }
    }

    public double[] read() {
        return decodedVector;
    }

    private void next() {
        if (count > 0) {
            count--;
            return;
        }

        value = vector[index++];

        if (value == Double.POSITIVE_INFINITY) {
            count = (int) vector[index++] - 1;
            value = vector[index++];
        }
    }

    private void reset() {
        this.index = 0;
        this.value = -1;
        this.count = -1;
    }

    private void compute() {
        for (int i = 0; i < decodedVector.length; i++) {
            next();
            decodedVector[i] = value;
        }
    }

}
