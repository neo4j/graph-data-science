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
package org.neo4j.gds.core.utils.paged;

import com.carrotsearch.hppc.ObjectDoubleHashMap;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class HugeLongLongDoubleMapTest {

    @Test
    void canReadFromAddTo() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap();
        map.addTo(1L, 1L, 1.0);

        double actual = map.getOrDefault(1L, 1L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        // different first key
        actual = map.getOrDefault(2L, 1L, 0.0);
        assertEquals(0.0, actual, 1e-4);

        // different second key
        actual = map.getOrDefault(1L, 2L, 0.0);
        assertEquals(0.0, actual, 1e-4);

        // different keys
        actual = map.getOrDefault(2L, 2L, 0.0);
        assertEquals(0.0, actual, 1e-4);
    }

    @Test
    void supportsTwoKeys() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap();
        map.addTo(1L, 1L, 1.0);
        map.addTo(1L, 2L, 2.0);
        map.addTo(2L, 1L, 3.0);
        map.addTo(2L, 2L, 4.0);

        double actual = map.getOrDefault(1L, 1L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        actual = map.getOrDefault(1L, 2L, 0.0);
        assertEquals(2.0, actual, 1e-4);

        actual = map.getOrDefault(2L, 1L, 0.0);
        assertEquals(3.0, actual, 1e-4);

        actual = map.getOrDefault(2L, 2L, 0.0);
        assertEquals(4.0, actual, 1e-4);
    }

    @Test
    void supportsZeroKeys() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap();

        map.addTo(0L, 0L, 1.0);
        double actual = map.getOrDefault(0L, 0L, 0.0);
        assertEquals(1.0, actual, 1e-4);

        map.addTo(0L, 2L, 2.0);
        actual = map.getOrDefault(0L, 0L, 0.0);
        assertEquals(1.0, actual, 1e-4);
        actual = map.getOrDefault(0L, 2L, 0.0);
        assertEquals(2.0, actual, 1e-4);

        map.addTo(2L, 0L, 4.0);
        actual = map.getOrDefault(0L, 0L, 0.0);
        assertEquals(1.0, actual, 1e-4);
        actual = map.getOrDefault(0L, 2L, 0.0);
        assertEquals(2.0, actual, 1e-4);
        actual = map.getOrDefault(2L, 0L, 0.0);
        assertEquals(4.0, actual, 1e-4);
    }

    @Test
    void addToAddsValues() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap();
        map.addTo(1L, 1L, 1.0);
        map.addTo(1L, 1L, 2.0);
        map.addTo(1L, 1L, 3.0);
        map.addTo(1L, 1L, 4.0);

        double actual = map.getOrDefault(1L, 1L, 0.0);
        assertEquals(10.0, actual, 1e-4);
    }

    @Test
    void hasSize() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap();
        assertEquals(0L, map.size());

        map.addTo(1L, 1L, 1.0);
        assertEquals(1L, map.size());

        map.addTo(2L, 2L, 2.0);
        assertEquals(2L, map.size());

        // same key
        map.addTo(1L, 1L, 2.0);
        assertEquals(2L, map.size());
    }

    @Test
    void hasIsEmpty() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap();
        assertTrue(map.isEmpty());
        map.addTo(1L, 1L, 1.0);
        assertFalse(map.isEmpty());
    }

    @Test
    void hasStringRepresentation() {
        HugeLongLongDoubleMap map = new HugeLongLongDoubleMap();
        ObjectDoubleHashMap<Pr> compare = new ObjectDoubleHashMap<>();

        assertEquals("[]", map.toString());

        for (long i = 0L; i < 20L; i++) {
            map.addTo(i, i * 42L, (double) i * 13.37);
            compare.put(new Pr(i, i * 42L), (double) i * 13.37);
        }

        // order is different, need to fake sort
        assertEquals(sortedToString(compare.toString()), sortedToString(map.toString()));
    }

    private static final Pattern COMMA_WS = Pattern.compile(", ");
    private static final Pattern ARROW = Pattern.compile("=>");
    private static final Pattern NON_DIGITS = Pattern.compile("\\D+");

    private static String sortedToString(String out) {
        return COMMA_WS.splitAsStream(out.substring(1, out.length() - 1))
                .sorted(HugeLongLongDoubleMapTest::comparePrEntry)
                .collect(Collectors.joining(", "));
    }

    private static int comparePrEntry(String key1, String key2) {
        int[] keys1 = getKeyPair(key1);
        int[] keys2 = getKeyPair(key2);
        for (int i = 0; i < keys1.length; i++) {
            int compare = Integer.compare(keys1[i], keys2[i]);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    private static int[] getKeyPair(String entry) {
        return ARROW.splitAsStream(entry)
                .limit(1L)
                .flatMap(NON_DIGITS::splitAsStream)
                .filter(s -> !s.isEmpty())
                .mapToInt(Integer::parseInt)
                .toArray();
    }

    static final class Pr {
        long k1;
        long k2;

        Pr(final long k1, final long k2) {
            this.k1 = k1;
            this.k2 = k2;
        }

        @Override
        public String toString() {
            return formatWithLocale("(%d,%d)", k1, k2);
        }

        @Override
        public int hashCode() {
            return (int) (k1 ^ k2);
        }
    }
}
