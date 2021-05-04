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

class ScalarTest {

    @Test
    void returnsScalarZero() {
       var scalar = new Scalar(5D);

        Scalar zero = scalar.zeros();

        assertNotSame(scalar, zero);
        assertArrayEquals(new double[] {0D}, zero.data);
    }

    @Test
    void copiesScalar() {
        var scalar = new Scalar(5D);

        Scalar copy = scalar.copy();

        assertNotSame(scalar, copy);
        assertArrayEquals(new double[] {5D}, copy.data);
    }

    @Test
    void addsScalar() {
        var scalar = new Scalar(5D);
        var scalarToAdd = new Scalar(2D);

        Scalar sum = scalar.add(scalarToAdd);

        assertNotSame(scalar, sum);
        assertNotSame(scalarToAdd, sum);
        assertArrayEquals(new double[] {7D}, sum.data);
    }
}
