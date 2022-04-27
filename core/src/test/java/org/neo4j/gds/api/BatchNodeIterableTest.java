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
package org.neo4j.gds.api;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class BatchNodeIterableTest {

    @Nested
    class BitSetIdIteratorTest {

        @Test
        void shouldIterateOverAllSetBits() {
           var bitSet = new BitSet(10);
           bitSet.set(0);
           bitSet.set(2);
           bitSet.set(4);

            var iter = new BatchNodeIterable.BitSetIdIterator(bitSet);

            var actual = new ArrayList<Long>();
            while (iter.hasNext()) {
                actual.add(iter.nextLong());
            }

            assertThat(actual).containsExactly(0L, 2L, 4L);
        }

        @Test
        void shouldNotFailForNoSetBits() {
            var bitSet = new BitSet(10);

            var iter = new BatchNodeIterable.BitSetIdIterator(bitSet);

            var actual = new ArrayList<Long>();
            while (iter.hasNext()) {
                actual.add(iter.nextLong());
            }

            assertThat(actual).isEmpty();
        }
    }
}
