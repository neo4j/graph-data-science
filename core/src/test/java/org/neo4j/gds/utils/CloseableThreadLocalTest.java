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
package org.neo4j.gds.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CloseableThreadLocalTest {
    private static final String TEST_VALUE = "initvaluetest";

    @Test
    void testInitValue() {
        try (CloseableThreadLocal<String> tl = new CloseableThreadLocal<>(() -> TEST_VALUE)) {
            var str = tl.get();
            assertEquals(TEST_VALUE, str);
        }
    }

    @Test
    void testNullValue() {
        // Tests that null can be set as a valid value (LUCENE-1805). This
        // previously failed in get().
        try (CloseableThreadLocal<Object> ctl = new CloseableThreadLocal<>(() -> null)) {
            ctl.set(null);
            assertNull(ctl.get());
        }
    }
}
