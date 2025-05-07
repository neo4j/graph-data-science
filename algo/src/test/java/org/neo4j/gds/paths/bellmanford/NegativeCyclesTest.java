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
package org.neo4j.gds.paths.bellmanford;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NegativeCyclesTest {

    @Test
    void shouldWork(){
        var negCycles = NegativeCycles.create(10,true);
        assertThat(negCycles.containsNegativeCycles()).isFalse();
        negCycles.considerAsStartNode(0);
        assertThat(negCycles.containsNegativeCycles()).isTrue();
        assertThat(negCycles.numberOfNegativeCycles()).isEqualTo(1L);
        negCycles.considerAsStartNode(1);
        assertThat(negCycles.containsNegativeCycles()).isTrue();
        assertThat(negCycles.numberOfNegativeCycles()).isEqualTo(2L);
        negCycles.considerAsStartNode(0);
        assertThat(negCycles.containsNegativeCycles()).isTrue();
        assertThat(negCycles.numberOfNegativeCycles()).isEqualTo(2L);
        var negs = negCycles.negativeCycles();
        assertThat(negs.get(0)).isEqualTo(0);
        assertThat(negs.get(1)).isEqualTo(1);

    }

}
