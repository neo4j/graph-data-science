/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class DuplicateRelationshipStrategyTest {

    private static final double[] inputs = new double[]{0.5, 1.4};

    @ParameterizedTest
    @CsvSource({"MAX, 1.4", "MIN, 0.5", "SKIP, 0.5", "SUM, 1.9"})
    public void testSuccessfulDuplateRelationshipStrategies(DuplicateRelationshipsStrategy strategy, double expected) {
        assertEquals(expected, strategy.merge(inputs[0], inputs[1]));
    }

    @ParameterizedTest
    @CsvSource({"DEFAULT", "NONE"})
    public void  testFailingDuplicateRelationshipStrategies(DuplicateRelationshipsStrategy strategy) {
        String expected = "Multiple relationships between the same pair of nodes are not expected. Try using SKIP or some other duplicate relationships strategy.";
        UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                () -> strategy.merge(42, 42));
        assertEquals(expected, exception.getMessage());
    }
}

