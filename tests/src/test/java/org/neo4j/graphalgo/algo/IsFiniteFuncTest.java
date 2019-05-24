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
package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.IsFiniteFunc;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IsFiniteFuncTest {
    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setUp() throws Exception {
        DB.resolveDependency(Procedures.class).registerFunction(IsFiniteFunc.class);
    }

    @Test
    public void isFinite() throws Exception {
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
    public void isInfinite() throws Exception {
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
    public void testInfinityAndNaN() throws Exception {
        double[] actual = DB.execute(
                "WITH [42, algo.Infinity(), 13.37, 0, algo.NaN(), 1.7976931348623157e308, -13] AS values RETURN filter(x IN values WHERE algo.isFinite(x)) as xs")
                .<List<Number>>columnAs("xs")
                .stream()
                .flatMap(Collection::stream)
                .mapToDouble(Number::doubleValue)
                .toArray();
        assertArrayEquals(new double[]{42, 13.37, 0, Double.MAX_VALUE, -13}, actual, 0.001);
    }

    private boolean callIsFinite(Number value) {
        return call(value, "algo.isFinite");
    }

    private boolean callIsInfinite(Number value) {
        return call(value, "algo.isInfinite");
    }

    private boolean call(Number value, String fun) {
        String query = "RETURN " + fun + "($value) as x";
        return DB.execute(query, singletonMap("value", value))
                .<Boolean>columnAs("x")
                .stream()
                .allMatch(Boolean::valueOf);
    }
}
