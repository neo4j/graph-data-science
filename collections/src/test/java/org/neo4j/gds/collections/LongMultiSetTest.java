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
package org.neo4j.gds.collections;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LongMultiSetTest {

    @Test
    void addValues() {
        var actualSet = new LongMultiSet();

        assertThat(actualSet)
            .matches(set -> set.keys().length == 0)
            .matches(set -> set.count(42) == 0);

        actualSet.add(42);
        actualSet.add(42);
        actualSet.add(1337, 30);
        actualSet.add(1337);

        assertThat(actualSet.keys()).containsExactlyInAnyOrder(42, 1337);

        assertThat(actualSet.count(42)).isEqualTo(2);
        assertThat(actualSet.count(1337)).isEqualTo(31);
        assertThat(actualSet.count(666)).isEqualTo(0);
    }

    @Test
    void sumShouldBeCorrect() {
        var longMultiSet = new LongMultiSet();
        longMultiSet.add(1, 10);
        longMultiSet.add(42, 43);
        assertThat(longMultiSet.sum()).isEqualTo(53);
    }

}
