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
package org.neo4j.gds.algorithms.community;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

 class CommunityCompanionTest {

    @Test
    void shouldAcceptLongPropertyTypes(){

        var nodePropertyValues = NodePropertyValuesAdapter.adapt(HugeLongArray.of(1,2,3,4));

        var graph = mock(Graph.class);
        when(graph.nodeProperties("foo")).thenReturn(nodePropertyValues);

        var extractedNodeProperties = CommunityCompanion.extractSeedingNodePropertyValues(graph,"foo");

        for (long i=0;i<4;++i) {
            assertThat(extractedNodeProperties.longValue(i)).isEqualTo(i+1);
        }

        }

        @Test
        void shouldThrowForNonLongValues(){
            var nodePropertyValues = NodePropertyValuesAdapter.adapt(HugeDoubleArray.of(1.5,2.5,3.5,4.5));

            var graph = mock(Graph.class);
            when(graph.nodeProperties("foo")).thenReturn(nodePropertyValues);

            assertThatThrownBy(()-> CommunityCompanion.extractSeedingNodePropertyValues(graph,"foo"))
                .hasMessageContaining(
                "Provided seeding property `foo`  does not comprise exclusively of long values");
        }
}
