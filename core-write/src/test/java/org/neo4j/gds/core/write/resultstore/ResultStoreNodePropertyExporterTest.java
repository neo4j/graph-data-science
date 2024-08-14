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
import org.neo4j.gds.api.ResultStoreEntry;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.write.NodeProperty;

import java.util.List;
import java.util.function.LongUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;

class ResultStoreNodePropertyExporterTest {

    @Test
    void shouldWriteSingleNodePropertyToResultStore() {
        var jobId = new JobId("test");
        var resultStore = new EphemeralResultStore();
        var nodePropertyValues = mock(NodePropertyValues.class);
        var toOriginalId = mock(LongUnaryOperator.class);
        when(nodePropertyValues.nodeCount()).thenReturn(42L);

        var nodePropertyExporter = new ResultStoreNodePropertyExporter(jobId, resultStore, List.of("A"), toOriginalId);
        nodePropertyExporter.write("prop", nodePropertyValues);

        assertThat(nodePropertyExporter.propertiesWritten()).isEqualTo(42L);

        var entry = resultStore.get(jobId);
        assertThat(entry).isInstanceOf(ResultStoreEntry.NodeProperties.class);

        var nodePropertiesEntry = (ResultStoreEntry.NodeProperties) entry;

        assertThat(nodePropertiesEntry.nodeLabels()).isEqualTo(List.of("A"));
        assertThat(nodePropertiesEntry.propertyKeys()).isEqualTo(List.of("prop"));
        assertThat(nodePropertiesEntry.propertyValues()).isEqualTo(List.of(nodePropertyValues));
        assertThat(nodePropertiesEntry.toOriginalId()).isEqualTo(toOriginalId);
    }

    @Test
    void shouldWriteMultipleNodePropertiesToResultStore() {
        var jobId = new JobId("test");
        var resultStore = new EphemeralResultStore();
        var nodePropertyValues1 = mock(NodePropertyValues.class);
        when(nodePropertyValues1.nodeCount()).thenReturn(42L);
        var nodePropertyValues2 = mock(NodePropertyValues.class);
        when(nodePropertyValues2.nodeCount()).thenReturn(43L);

        var toOriginalId = mock(LongUnaryOperator.class);

        var nodePropertyExporter = new ResultStoreNodePropertyExporter(jobId, resultStore, List.of(PROJECT_ALL), toOriginalId);
        nodePropertyExporter.write(List.of(
                NodeProperty.of("prop1", nodePropertyValues1),
                NodeProperty.of("prop2", nodePropertyValues2)
            )
        );

        assertThat(nodePropertyExporter.propertiesWritten()).isEqualTo(85L);

        var entry = resultStore.get(jobId);
        assertThat(entry).isInstanceOf(ResultStoreEntry.NodeProperties.class);

        var nodePropertiesEntry = (ResultStoreEntry.NodeProperties) entry;

        assertThat(nodePropertiesEntry.nodeLabels()).isEqualTo(List.of(PROJECT_ALL));
        assertThat(nodePropertiesEntry.propertyKeys()).isEqualTo(List.of("prop1", "prop2"));
        assertThat(nodePropertiesEntry.propertyValues()).isEqualTo(List.of(nodePropertyValues1, nodePropertyValues2));
        assertThat(nodePropertiesEntry.toOriginalId()).isEqualTo(toOriginalId);
    }
}
