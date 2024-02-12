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
package org.neo4j.gds.paths.dijkstra;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TargetsTest {

    @Test
    void shouldWorkUnspecifiedTargets(){
            var targets = Targets.of(List.of());
            assertThat(targets.apply(0)).isEqualTo(TraversalState.EMIT_AND_CONTINUE);
            assertThat(targets.apply(1)).isEqualTo(TraversalState.EMIT_AND_CONTINUE);
    }
    @Test
    void shouldWorkForSingleTarget(){
        var targets = Targets.of(List.of(3L));
        assertThat(targets.apply(0)).isEqualTo(TraversalState.CONTINUE);
        assertThat(targets.apply(3)).isEqualTo(TraversalState.EMIT_AND_STOP);
    }

    @Test
    void shouldWorkForManyTargets(){
        var targets = Targets.of(List.of(3L,40L));
        assertThat(targets.apply(100)).isEqualTo(TraversalState.CONTINUE);
        assertThat(targets.apply(3)).isEqualTo(TraversalState.EMIT_AND_CONTINUE);
        assertThat(targets.apply(1)).isEqualTo(TraversalState.CONTINUE);
        assertThat(targets.apply(40)).isEqualTo(TraversalState.EMIT_AND_STOP);

    }

}
