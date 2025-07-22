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
package org.neo4j.gds.cliqueCounting;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecursiveCliqueNodesTest {

    @Test
    void shouldOperate(){
        var cliques = new RecursiveCliqueNodes(100);
        cliques.add(0,true);
        assertThat(cliques.requiredNodes()).isEqualTo(1L);
        cliques.add(1,false);
        assertThat(cliques.requiredNodes()).isEqualTo(1L);
        cliques.add(2,true);
        assertThat(cliques.requiredNodes()).isEqualTo(2L);
        cliques.add(3,true);
        assertThat(cliques.requiredNodes()).isEqualTo(3L);
        assertThat(cliques.activeNodes()).containsExactly(0,2,3,1);
        cliques.finishRecursionLevel();
        assertThat(cliques.requiredNodes()).isEqualTo(2L);
        assertThat(cliques.activeNodes()).containsExactly(0,2,1);
        cliques.finishRecursionLevel();
        assertThat(cliques.requiredNodes()).isEqualTo(1L);
        assertThat(cliques.activeNodes()).containsExactly(0,1);
        cliques.finishRecursionLevel();
        assertThat(cliques.requiredNodes()).isEqualTo(1L);
        assertThat(cliques.activeNodes()).containsExactly(0);

    }

}
