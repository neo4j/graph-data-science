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


class TensorFactoryTest {

    @Test
    void canBuildTensors() {
        Tensor<?> scalar = TensorFactory.constant(1, new int[]{1});
        Tensor<?> vector = TensorFactory.constant(1, new int[]{2});
        Tensor<?> matrix = TensorFactory.constant(1, new int[]{3, 4});


        assertThat(scalar)
            .isInstanceOf(Scalar.class)
            .isEqualTo(new Scalar(1));

        assertThat(vector)
            .isInstanceOf(Vector.class)
            .isEqualTo(new Vector(new double[]{1,1}));

        assertThat(matrix)
            .isInstanceOf(Matrix.class)
            .isEqualTo(Matrix.create(1, 3, 4));
    }

    @Test
    void throwsOnWrongDimensions() {
        assertThrows(IllegalArgumentException.class, () -> TensorFactory.constant(1, new int[]{1, 2, 3}));
        assertThrows(IllegalArgumentException.class, () -> TensorFactory.constant(1, new int[]{-1}));
        assertThrows(IllegalArgumentException.class, () -> TensorFactory.constant(1, new int[]{-1, 0}));
        assertThrows(IllegalArgumentException.class, () -> TensorFactory.constant(1, new int[]{10, 0}));
    }
}
