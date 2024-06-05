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
import org.neo4j.gds.core.utils.progress.JobId;

import java.util.function.LongUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

class ResultStoreNodeLabelExporterTest {

    @Test
    void shouldWriteToResultStore() {
        var jobId = new JobId("test");
        var resultStore = new EphemeralResultStore();
        LongUnaryOperator toOriginalId = l -> l + 42;
        var nodeLabelExporter = new ResultStoreNodeLabelExporter(jobId, resultStore, 5, toOriginalId);
        nodeLabelExporter.write("label");

        assertThat(nodeLabelExporter.nodeLabelsWritten()).isEqualTo(5);

        var nodeLabelEntry = resultStore.getNodeIdsByLabel("label");
        assertThat(nodeLabelEntry.nodeCount()).isEqualTo(5);

        for (int i = 0; i < 5; i++) {
            assertThat(nodeLabelEntry.toOriginalId().applyAsLong(i)).isEqualTo(i + 42);
        }

        var entry = resultStore.get(jobId);
        assertThat(entry).isInstanceOf(ResultStoreEntry.NodeLabel.class);

        var jobIdNodeLabelEntry = (ResultStoreEntry.NodeLabel) entry;
        assertThat(jobIdNodeLabelEntry.nodeLabel()).isEqualTo("label");
        assertThat(jobIdNodeLabelEntry.nodeCount()).isEqualTo(5);
    }

}
