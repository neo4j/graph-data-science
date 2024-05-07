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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.ResultStoreEntry;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.write.RelationshipExporter;

import java.util.function.LongUnaryOperator;

public class ResultStoreRelationshipExporter implements RelationshipExporter {

    private final JobId jobId;
    private final ResultStore resultStore;
    private final Graph graph;
    private final LongUnaryOperator toOriginalId;

    ResultStoreRelationshipExporter(
        JobId jobId,
        ResultStore resultStore,
        Graph graph,
        LongUnaryOperator toOriginalId
    ) {
        this.jobId = jobId;
        this.resultStore = resultStore;
        this.graph = graph;
        this.toOriginalId = toOriginalId;
    }

    @Override
    public void write(String relationshipType) {
        resultStore.addRelationship(relationshipType, graph, toOriginalId);
        resultStore.add(jobId, new ResultStoreEntry.RelationshipTopology(relationshipType, graph, toOriginalId));
    }

    @Override
    public void write(String relationshipType, String propertyKey) {
        write(relationshipType, propertyKey, null);
    }

    @Override
    public void write(
        String relationshipType,
        String propertyKey,
        @Nullable RelationshipWithPropertyConsumer afterWriteConsumer
    ) {
        resultStore.addRelationship(relationshipType, propertyKey, graph, toOriginalId);
        resultStore.add(jobId, new ResultStoreEntry.RelationshipsFromGraph(relationshipType, propertyKey, graph, toOriginalId));
    }
}
