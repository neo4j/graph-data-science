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
package org.neo4j.gds.core.write.resultstore;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.EphemeralResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.write.ImmutableNodeProperty;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;

class ResultStoreNodePropertyExporterTest {

    @Test
    void shouldWriteSingleNodePropertyToResultStore() {
        var resultStore = new EphemeralResultStore();
        var nodePropertyValues = mock(NodePropertyValues.class);
        when(nodePropertyValues.nodeCount()).thenReturn(42L);

        var nodePropertyExporter = new ResultStoreNodePropertyExporter(resultStore, List.of("A"));
        nodePropertyExporter.write("prop", nodePropertyValues);

        assertThat(nodePropertyExporter.propertiesWritten()).isEqualTo(42L);
        assertThat(resultStore.getNodePropertyValues(List.of("A"), "prop")).isEqualTo(nodePropertyValues);

        var newNodePropertyValues = mock(NodePropertyValues.class);
        when(newNodePropertyValues.nodeCount()).thenReturn(43L);

        var newNodePropertyExporter1 = new ResultStoreNodePropertyExporter(resultStore, List.of("A", "B"));
        newNodePropertyExporter1.write( ImmutableNodeProperty.of("newProp", newNodePropertyValues));

        assertThat(newNodePropertyExporter1.propertiesWritten()).isEqualTo(43L);
        assertThat(resultStore.getNodePropertyValues(List.of("A", "B"), "newProp")).isEqualTo(newNodePropertyValues);
    }

    @Test
    void shouldWriteMultipleNodePropertiesToResultStore() {
        var resultStore = new EphemeralResultStore();
        var nodePropertyValues1 = mock(NodePropertyValues.class);
        when(nodePropertyValues1.nodeCount()).thenReturn(42L);
        var nodePropertyValues2 = mock(NodePropertyValues.class);
        when(nodePropertyValues2.nodeCount()).thenReturn(43L);

        var nodePropertyExporter = new ResultStoreNodePropertyExporter(resultStore, List.of(PROJECT_ALL));
        nodePropertyExporter.write(List.of(
                ImmutableNodeProperty.of("prop1", nodePropertyValues1),
                ImmutableNodeProperty.of("prop2", nodePropertyValues2)
            )
        );

        assertThat(nodePropertyExporter.propertiesWritten()).isEqualTo(85L);
        assertThat(resultStore.getNodePropertyValues(List.of(PROJECT_ALL), "prop1")).isEqualTo(nodePropertyValues1);
        assertThat(resultStore.getNodePropertyValues(List.of(PROJECT_ALL), "prop2")).isEqualTo(nodePropertyValues2);
    }
}
