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
package org.neo4j.gds.pagerank;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceBasedRestartProbabilityTest {

    @Test
    void testSingleEntry(){
        var alpha = 0.2;
        var map = Map.of(1L,3.0);
        var sourceBasedRestartProbability = new SourceBasedRestartProbability(alpha, map);

        assertThat(sourceBasedRestartProbability.provideInitialValue(1)).isEqualTo(0.2*3.0);
        assertThat(sourceBasedRestartProbability.provideInitialValue(2)).isEqualTo(0);
    }

    @Test
    void testNegativeEntry(){
        var alpha = 0.2;
        var map = Map.of(1L, 3.0, 5L, -0.1);
        assertThatThrownBy(() -> new SourceBasedRestartProbability(alpha, map));
    }
}
