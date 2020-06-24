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
import org.neo4j.gds.embeddings.graphsage.ddl4j.GraphSageBaseTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.FiniteDifferenceTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogarithmTest extends GraphSageBaseTest implements FiniteDifferenceTest {
    @Test
    void testApply() {
        Variable variable = new Weights(Tensor.scalar(5D));
        Logarithm logarithm = new Logarithm(variable);

        assertEquals(Math.log(5D), ctx.forward(logarithm).data[0]);
    }

    @Test
    void shouldApproximateGradient() {
        Weights scalar1 = new Weights(Tensor.scalar(5D));
        Weights scalar2 = new Weights(Tensor.scalar(0.001));
        finiteDifferenceShouldApproximateGradient(scalar1, new Logarithm(scalar1));
        finiteDifferenceShouldApproximateGradient(scalar2, new Logarithm(scalar2));
    }

    // this shows why implementing differentiation with finite difference would be problematic
    @Override
    public double epsilon() {
        return 1e-10;
    }

    @Override
    public double tolerance() {
        return 1e-4;
    }
}
