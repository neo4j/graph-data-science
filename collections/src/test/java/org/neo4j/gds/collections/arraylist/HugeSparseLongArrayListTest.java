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
package org.neo4j.gds.collections.arraylist;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class HugeSparseLongArrayListTest {

    @Test
    void shouldSetAndGet() {
        var random = ThreadLocalRandom.current();
        var array = HugeSparseLongArrayList.of(42L);
        long index = random.nextLong(0, 4096L + 1337L);
        long value = random.nextLong();
        array.set(index, value);
        assertThat(array.get(index)).isEqualTo(value);
    }

    @Test
    void shouldOnlySetIfAbsent() {
        var random = ThreadLocalRandom.current();
        var array = HugeSparseLongArrayList.of(42L);
        long index = random.nextLong(0, 4096L + 1337L);
        long value = 1337L;
        assertThat(array.setIfAbsent(index, value)).isTrue();
        assertThat(array.setIfAbsent(index, value)).isFalse();
    }

    @Test
    void shouldAddToAndGet() {
        var random = ThreadLocalRandom.current();
        var array = HugeSparseLongArrayList.of(42L);
        long index = random.nextLong(0, 4096L + 1337L);
        long value = 42L;
        array.addTo(index, value);
        array.addTo(index, value);
        assertThat(array.get(index)).isEqualTo(42L + 42L + 42L);
    }

    @Test
    void shouldAddToAndGetWithDefaultZero() {
        var random = ThreadLocalRandom.current();
        var array = HugeSparseLongArrayList.of(0L);
        long index = random.nextLong(0, 4096L + 1337L);
        long value = 42L;
        array.addTo(index, value);
        array.addTo(index, value);
        assertThat(array.get(index)).isEqualTo(42L + 42L);
    }

    @Test
    void shouldReturnDefaultValue() {
        var array = HugeSparseLongArrayList.of(42L);
        // > PAGE_SIZE;
        var index = 224242;
        array.set(index, 1337L);
        for (long i = 0; i < index; i++) {
            assertThat(array.get(i)).isEqualTo(42L);
        }
        assertThat(array.get(index)).isEqualTo(1337L);
    }

    @Test
    void shouldHaveSaneCapacity() {
        var array = HugeSparseLongArrayList.of(42L);
        // > PAGE_SIZE;
        var index = 224242;
        long value = 1337L;
        array.set(index, value);
        assertThat(array.capacity()).isGreaterThanOrEqualTo(index);
    }

    @Test
    void shouldReportContainsCorrectly() {
        var random = ThreadLocalRandom.current();
        var array = HugeSparseLongArrayList.of(42L);

        long index = random.nextLong(0, 4096L + 1337L);
        long value = 1337L;
        array.set(index, value);
        assertThat(array.contains(index)).isTrue();
        assertThat(array.contains(index + 1)).isFalse();
    }

    @Test
    void forAll() {
        var array = HugeSparseLongArrayList.of(42L);

        var expected = Map.of(42L, 1337L, 42 + 8192L, 1338L, 42 + 8192 + 4096L, 1338L);
        expected.forEach(array::set);

        var actual = new HashMap<>();
        array.forAll(actual::put);

        assertThat(actual).isEqualTo(expected);
    }

}
