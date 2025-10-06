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
package org.neo4j.gds.procedures.algorithms.pathfinding.write;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.JobId;

import java.util.concurrent.atomic.AtomicLong;

public final class WriteStepExecute {

    private WriteStepExecute() {}

    static <RESULT_FROM_ALGORITHM> WriteRelationshipMetadata executeWriteRelationshipStep(
        WriteStep<RESULT_FROM_ALGORITHM,RelationshipsWritten> writeStep,
        Graph graph,
        GraphStore graphStore,
        JobId jobId,
        RESULT_FROM_ALGORITHM algorithmResult,
        ResultStore  resultStore
    ) {
        RelationshipsWritten relationshipsWritten;
        var writeMillis = new AtomicLong();
        try (var ignored = ProgressTimer.start(writeMillis::set)) {
            relationshipsWritten = writeStep.execute(
                graph,
                graphStore,
                resultStore,
                algorithmResult,
                jobId
            );
        }
        return new WriteRelationshipMetadata(relationshipsWritten.value(), writeMillis.get());
    }
}
