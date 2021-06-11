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
package org.neo4j.gds.ml.core.la.operations;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FloatVectorOperationsTest {

    @Test
    void add() {
        float[] a = {3.5f, -2f, 1f};
        float[] b = {5f, 2f, 2.6f};

        FloatVectorOperations.addInPlace(a, b);

        assertThat(a).containsExactly(8.5f, 0f, 3.6f);
    }

    @Test
    void addVectorsOfDifferentLength() {
        float[] a = {5f, 2f, 2.6f};
        float[] b = {3.5f};

        FloatVectorOperations.addInPlace(a, b);

        assertThat(a).containsExactly(8.5f, 2f, 2.6f);
    }

    @Test
    void scale() {
        float[] a = {2f, 4.2f};

        FloatVectorOperations.scale(a, 2.5f);

        assertThat(a).containsExactly(5f, 10.5f);
    }

    @Test
    void addWeighted() {
        float[] a = {3.5f, -2f, 1f};
        float[] expected = a.clone();
        float[] b = {5f, 2f, 2.6f};
        float scalar = 42.42f;


        FloatVectorOperations.addWeightedInPlace(a, b, scalar);

        FloatVectorOperations.scale(b, scalar);
        FloatVectorOperations.addInPlace(expected, b);

        assertThat(a).containsExactly(expected);
    }

    @Test
    void l2Normalize() {
        float[] a = {4f, -2.5f, 3.3f};

        FloatVectorOperations.l2Normalize(a);
        float l2 = (float) Math.sqrt(33.14f);

        assertThat(a).containsExactly(4f / l2, -2.5f / l2, 3.3f / l2);
    }

}