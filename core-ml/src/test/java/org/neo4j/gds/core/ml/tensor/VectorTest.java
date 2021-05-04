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
package org.neo4j.gds.core.ml.tensor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;


class VectorTest {

    @Test
    void returnsVectorZero() {
        Vector vector = Vector.fill(3D, 5);

        Vector zeros = vector.zeros();

        assertNotSame(vector, zeros);
        assertArrayEquals(new double[] {0, 0, 0, 0, 0}, zeros.data);
    }

    @Test
    void copiesVector() {
        Vector vector = new Vector(new double[] { 3D, 2D, 1D});

        Vector copy = vector.copy();

        assertNotSame(vector, copy);
        assertArrayEquals(vector.data, copy.data);
        assertNotSame(vector.data, copy.data);
    }

    @Test
    void copiesEmptyVector() {
        Vector vector = new Vector(new double[0]);

        Vector copy = vector.copy();

        assertNotSame(vector, copy);
        assertArrayEquals(new double[] {}, copy.data);
    }

    @Test
    void addsVector() {
        Vector vector = new Vector(new double[] { 3D, 2D, 1D});
        Vector vectorToAdd = new Vector(new double[] { 4D, 6D, 8D});

        Vector sum = vector.add(vectorToAdd);

        assertNotSame(vector, sum);
        assertNotSame(vectorToAdd, sum);

        assertArrayEquals(new double[]{7D, 8D, 9D}, sum.data);
    }

    @Test
    void failsOnDimensionsMismatch() {
        Vector vector = new Vector(new double[] { 3D, 2D, 1D});
        Vector vectorToAdd = new Vector(new double[] { 4D });

        assertThrows(ArithmeticException.class, () -> vector.add(vectorToAdd));
    }

}
