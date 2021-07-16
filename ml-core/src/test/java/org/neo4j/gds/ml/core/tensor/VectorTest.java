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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;


class VectorTest {

    @Test
    void returnsVectorZero() {
        Vector vector = Vector.create(3D, 5);

        Vector zeros = vector.zeros();

        assertThat(zeros)
            .isNotSameAs(vector)
            .isEqualTo(new Vector(0, 0, 0, 0, 0));
    }

    @Test
    void copiesVector() {
        Vector vector = new Vector(3D, 2D, 1D);

        Vector copy = vector.copy();

        assertThat(copy)
            .isNotSameAs(vector)
            .isEqualTo(vector);

        // test internal objects was also copied
        copy.setDataAt(2, 42);
        assertThat(copy).isNotEqualTo(vector);
    }

    @Test
    void copiesEmptyVector() {
        Vector vector = new Vector();

        Vector copy = vector.copy();

        assertThat(copy)
            .isNotSameAs(vector)
            .isEqualTo(vector);
    }

    @Test
    void addsVector() {
        Vector vector = new Vector(3D, 2D, 1D);
        Vector vectorToAdd = new Vector(4D, 6D, 8D);

        Vector sum = vector.add(vectorToAdd);

        assertThat(sum)
            .isNotSameAs(vector)
            .isNotSameAs(vectorToAdd)
            .isEqualTo(new Vector(7D, 8D, 9D));
    }

    @Test
    void failsOnDimensionsMismatch() {
        Vector vector = new Vector(3D, 2D, 1D);
        Vector vectorToAdd = new Vector(4D);

        assertThrows(ArithmeticException.class, () -> vector.add(vectorToAdd));
    }

}
