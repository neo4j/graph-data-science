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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AggregationTest {

    private static final double[] inputs = new double[]{0.5, 1.4};

    @ParameterizedTest
    @CsvSource({"MAX, 1.4", "MIN, 0.5", "SINGLE, 0.5", "SUM, 1.9", "COUNT, 2.0"})
    void testSuccessfulDuplateRelationshipStrategies(Aggregation strategy, double expected) {
        assertEquals(expected, strategy.merge(inputs[0], inputs[1]));
    }

    @Test
    void testFailingDuplicateRelationshipStrategies() {
        UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                () -> Aggregation.NONE.merge(42, 42));
        String expected = "Multiple relationships between the same pair of nodes are not expected. Try using SKIP or some other aggregation.";
        assertEquals(expected, exception.getMessage());
    }

    @Test
    void testFailingDefaultDuplicateRelationshipStrategies() {
        UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                () -> Aggregation.DEFAULT.merge(42, 42));
        String expected = "This should never be used as a valid aggregation, just as a placeholder for the default aggregation of a given loader.";
        assertEquals(expected, exception.getMessage());
    }
}

