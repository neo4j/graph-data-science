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
package org.neo4j.gds.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SourceNodesFactoryTest {

    @Test
    void testSourceNodesListOutputMapping(){
        var sourceNodes = SourceNodesFactory.parse(List.of(1L, 42L));
        assertThat(SourceNodesFactory.toMapOutput(sourceNodes)).containsExactly(1L,42L);
    }

    @Test()
    void testSourceNodesListOfPairsToString(){

        var sourceNodes = SourceNodesFactory.parse(List.of(List.of(3L, 0.1D), List.of(10L, 2D)));
        assertThat(SourceNodesFactory.toMapOutput(sourceNodes)).containsExactlyInAnyOrder(List.of(3L, 0.1D), List.of(10L, 2D));
    }

    @Test
    void testListSourceNodes() {
        var listSourceNodes = SourceNodesFactory.parse(List.of(1,42));
        assertThat(listSourceNodes).isInstanceOf(ListSourceNodes.class);
        assertThat(listSourceNodes.sourceNodes()).containsExactly(1L,42L);
    }

    @Test
    void testMapSourceNodesFromListOfPairs() {
        var mapSourceNodes = SourceNodesFactory.parse(List.of(List.of(2L, 0.5D), List.of(42L, 3.0D)));
        assertThat(mapSourceNodes).isInstanceOf(MapSourceNodes.class);
        assertThat(mapSourceNodes.sourceNodes()).containsExactly(2L, 42L);
    }


}
