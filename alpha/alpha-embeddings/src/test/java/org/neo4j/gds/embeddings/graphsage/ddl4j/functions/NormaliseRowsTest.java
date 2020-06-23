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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.AutogradBaseTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.FiniteDifferenceTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class NormaliseRowsTest extends AutogradBaseTest implements FiniteDifferenceTest {
    @Override
    public double epsilon() {
        return 1e-6;
    }

    @Test
    void testGradient() {
        double[] data = new double[] {
            3, 4,
            1, 0,
            0, 1
        };

        Weights w = new Weights(Tensor.matrix(data, 3, 2));
        NormaliseRows normaliseRows = new NormaliseRows(w);

        finiteDifferenceShouldApproximateGradient(w, new Sum(List.of(normaliseRows)));
    }

    @Test
    void testApply() {
        double[] data = new double[]{
            3, 4,
            1, 0,
            0, 1
        };
        double[] expectedData = new double[]{
            3.0/5, 4.0/5,
            1, 0,
            0, 1
        };
        Weights w = new Weights(Tensor.matrix(data, 3, 2));
        NormaliseRows normaliseRows = new NormaliseRows(w);
        assertArrayEquals(expectedData, ctx.forward(normaliseRows).data);
    }


}