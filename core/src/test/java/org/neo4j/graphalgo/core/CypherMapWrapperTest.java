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
import org.neo4j.graphalgo.compat.MapUtil;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CypherMapWrapperTest {

    @Test
    void testCastingFromNumberToDouble() {
        Map<String, Object> numberPrimitives = MapUtil.map(
            "integer", 42,
            "long", 1337L,
            "float", 1337.42f
        );
        CypherMapWrapper primitivesWrapper = CypherMapWrapper.create(numberPrimitives);
        assertEquals(42D, primitivesWrapper.getDouble("integer", 0.0D));
        assertEquals(1337D, primitivesWrapper.getDouble("long", 0.0D));
        assertEquals(1337.42D, primitivesWrapper.getDouble("float", 0.0D), 0.0001D);
    }

    @Test
    void shouldFailOnLossyCasts() {
        Map<String, Object> numberPrimitives = MapUtil.map(
            "double", 1337.42D
        );
        CypherMapWrapper primitivesWrapper = CypherMapWrapper.create(numberPrimitives);

        IllegalArgumentException doubleEx = assertThrows(
            IllegalArgumentException.class,
            () -> primitivesWrapper.getLong("double", 0)
        );

        assertTrue(doubleEx.getMessage().contains("must be of type `Long` but was `Double`"));
    }
}