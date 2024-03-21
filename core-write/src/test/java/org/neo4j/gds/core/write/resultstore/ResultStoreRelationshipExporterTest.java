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
import org.neo4j.gds.api.Graph;

import java.util.function.LongUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ResultStoreRelationshipExporterTest {

    @Test
    void shouldWriteRelationshipWithoutPropertyToGraphStore() {
        var resultStore = new EphemeralResultStore();
        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);
        new ResultStoreRelationshipExporter(resultStore, graph, toOriginalId).write("REL");

        var relationshipEntry = resultStore.getRelationship("REL");
        assertThat(relationshipEntry.graph()).isEqualTo(graph);
        assertThat(relationshipEntry.toOriginalId()).isEqualTo(toOriginalId);

        assertThat(resultStore.getRelationship("REL", "foo")).isNull();
    }

    @Test
    void shouldWriteRelationshipWithPropertyToGraphStore() {
        var resultStore = new EphemeralResultStore();
        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);
        new ResultStoreRelationshipExporter(resultStore, graph, toOriginalId).write("REL", "prop");

        var relationshipEntry = resultStore.getRelationship("REL", "prop");
        assertThat(relationshipEntry.graph()).isEqualTo(graph);
        assertThat(relationshipEntry.toOriginalId()).isEqualTo(toOriginalId);

        assertThat(resultStore.getRelationship("REL")).isNull();
    }
}
