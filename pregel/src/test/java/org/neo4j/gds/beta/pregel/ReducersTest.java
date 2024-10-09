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
package org.neo4j.gds.beta.pregel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class ReducersTest {

    @Test
    void parse() {
        assertThat(Reducers.parse("SUM")).isInstanceOf(Reducer.Sum.class);
        assertThat(Reducers.parse("MIN")).isInstanceOf(Reducer.Min.class);
        assertThat(Reducers.parse("MAX")).isInstanceOf(Reducer.Max.class);
        assertThat(Reducers.parse("COUNT")).isInstanceOf(Reducer.Count.class);
    }

    @Test
    void parseUnknownReducer() {
        assertThatThrownBy(() -> Reducers.parse("UNKNOWN"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unknown reducer: `UNKNOWN`");
    }

    @Test
    void testToString() {
        assertThat(Reducers.toString(new Reducer.Sum())).isEqualTo("SUM");
        assertThat(Reducers.toString(new Reducer.Min())).isEqualTo("MIN");
        assertThat(Reducers.toString(new Reducer.Max())).isEqualTo("MAX");
        assertThat(Reducers.toString(new Reducer.Count())).isEqualTo("COUNT");
    }
}
