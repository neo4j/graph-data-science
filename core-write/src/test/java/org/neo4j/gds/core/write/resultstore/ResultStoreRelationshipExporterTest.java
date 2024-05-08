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
import org.neo4j.gds.api.ResultStoreEntry;
import org.neo4j.gds.core.utils.progress.JobId;

import java.util.function.LongUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ResultStoreRelationshipExporterTest {

    @Test
    void shouldWriteRelationshipWithoutPropertyToResultStore() {
        var jobId = new JobId("test");
        var resultStore = new EphemeralResultStore();
        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);
        new ResultStoreRelationshipExporter(jobId, resultStore, graph, toOriginalId).write("REL");

        var relationshipEntry = resultStore.getRelationship("REL");
        assertThat(relationshipEntry.graph()).isEqualTo(graph);
        assertThat(relationshipEntry.toOriginalId()).isEqualTo(toOriginalId);

        assertThat(resultStore.getRelationship("REL", "foo")).isNull();

        var entry = resultStore.get(jobId);
        assertThat(entry).isInstanceOf(ResultStoreEntry.RelationshipTopology.class);

        var jobIdRelationshipEntry = (ResultStoreEntry.RelationshipTopology) entry;
        assertThat(jobIdRelationshipEntry.relationshipType()).isEqualTo("REL");
        assertThat(jobIdRelationshipEntry.graph()).isEqualTo(graph);
        assertThat(jobIdRelationshipEntry.toOriginalId()).isEqualTo(toOriginalId);
    }

    @Test
    void shouldWriteRelationshipWithPropertyToResultStore() {
        var jobId = new JobId("test");
        var resultStore = new EphemeralResultStore();
        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);
        new ResultStoreRelationshipExporter(jobId, resultStore, graph, toOriginalId).write("REL", "prop");

        var relationshipEntry = resultStore.getRelationship("REL", "prop");
        assertThat(relationshipEntry.graph()).isEqualTo(graph);
        assertThat(relationshipEntry.toOriginalId()).isEqualTo(toOriginalId);

        assertThat(resultStore.getRelationship("REL")).isNull();

        var entry = resultStore.get(jobId);
        assertThat(entry).isInstanceOf(ResultStoreEntry.RelationshipsFromGraph.class);

        var jobIdRelationshipEntry = (ResultStoreEntry.RelationshipsFromGraph) entry;

        assertThat(jobIdRelationshipEntry.relationshipType()).isEqualTo("REL");
        assertThat(jobIdRelationshipEntry.propertyKey()).isEqualTo("prop");
        assertThat(jobIdRelationshipEntry.graph()).isEqualTo(graph);
        assertThat(jobIdRelationshipEntry.toOriginalId()).isEqualTo(toOriginalId);
    }
}
