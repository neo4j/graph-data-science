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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IsFiniteFuncTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerFunctions(IsFiniteFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void isFinite() {
        assertFalse(callIsFinite(null));
        assertFalse(callIsFinite(Double.NaN));
        assertFalse(callIsFinite(Double.POSITIVE_INFINITY));
        assertFalse(callIsFinite(Double.NEGATIVE_INFINITY));
        assertFalse(callIsFinite(Float.NaN));
        assertFalse(callIsFinite(Float.POSITIVE_INFINITY));
        assertFalse(callIsFinite(Float.NEGATIVE_INFINITY));
        assertTrue(callIsFinite(0L));
        assertTrue(callIsFinite(42.1337));
        assertTrue(callIsFinite(Double.MAX_VALUE));
        assertTrue(callIsFinite(Double.MIN_VALUE));
        assertTrue(callIsFinite(Long.MAX_VALUE));
        assertTrue(callIsFinite(Long.MIN_VALUE));
    }

    @Test
    void isInfinite() {
        assertTrue(callIsInfinite(null));
        assertTrue(callIsInfinite(Double.NaN));
        assertTrue(callIsInfinite(Double.POSITIVE_INFINITY));
        assertTrue(callIsInfinite(Double.NEGATIVE_INFINITY));
        assertTrue(callIsInfinite(Float.NaN));
        assertTrue(callIsInfinite(Float.POSITIVE_INFINITY));
        assertTrue(callIsInfinite(Float.NEGATIVE_INFINITY));
        assertFalse(callIsInfinite(0L));
        assertFalse(callIsInfinite(42.1337));
        assertFalse(callIsInfinite(Double.MAX_VALUE));
        assertFalse(callIsInfinite(Double.MIN_VALUE));
        assertFalse(callIsInfinite(Long.MAX_VALUE));
        assertFalse(callIsInfinite(Long.MIN_VALUE));
    }

    @Test
    void testInfinityAndNaN() {
        double[] actual = runQuery(
            "WITH [42, gds.util.Infinity(), 13.37, 0, gds.util.NaN(), 1.7976931348623157e308, -13] AS values RETURN filter(x IN values WHERE gds.util.isFinite(x)) as xs",
            result -> result.<List<Number>>columnAs("xs")
                .stream()
                .flatMap(Collection::stream)
                .mapToDouble(Number::doubleValue)
                .toArray()
        );
        assertArrayEquals(new double[]{42, 13.37, 0, Double.MAX_VALUE, -13}, actual, 0.001);
    }

    private boolean callIsFinite(Number value) {
        return call(value, "gds.util.isFinite");
    }

    private boolean callIsInfinite(Number value) {
        return call(value, "gds.util.isInfinite");
    }

    private boolean call(Number value, String fun) {
        String query = "RETURN " + fun + "($value) as x";

        return runQuery(query, singletonMap("value", value), result -> result
            .<Boolean>columnAs("x")
            .stream()
            .allMatch(Boolean::valueOf)
        );
    }
}
