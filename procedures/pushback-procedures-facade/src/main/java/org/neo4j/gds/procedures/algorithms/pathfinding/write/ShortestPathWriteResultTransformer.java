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
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.pathfinding.ShortestPathWriteStep;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

class ShortestPathWriteResultTransformer implements ResultTransformer<TimedAlgorithmResult<PathFindingResult>, Stream<StandardWriteRelationshipsResult>> {
    private final ShortestPathWriteStep writeStep;
    private final Graph graph;
    private final GraphStore graphStore;
    @Deprecated(forRemoval = true)
    private final ResultStore resultStore;
    private final JobId jobId;
    private final Map<String, Object> configurationMap;

    ShortestPathWriteResultTransformer(
        ShortestPathWriteStep writeStep,
        Graph graph,
        GraphStore graphStore,
        @Deprecated(forRemoval = true)
        ResultStore resultStore,
        JobId jobId,
        Map<String, Object> configurationMap
    ) {
        this.writeStep = writeStep;
        this.graph = graph;
        this.graphStore = graphStore;
        this.resultStore = resultStore;
        this.jobId = jobId;
        this.configurationMap = configurationMap;
    }

    @Override
    public Stream<StandardWriteRelationshipsResult> apply(TimedAlgorithmResult<PathFindingResult> algorithmResult) {

        var result = algorithmResult.result();
        var writeRelationshipsMetadata = WriteStepExecute.executeWriteRelationshipStep(
            writeStep,
            graph,
            graphStore,
            jobId,
            result,
            resultStore
        );

        return Stream.of(
            new StandardWriteRelationshipsResult(
                0,
                algorithmResult.computeMillis(),
                0,
                writeRelationshipsMetadata.writeMillis(),
                writeRelationshipsMetadata.relationshipsWritten(),
                configurationMap
            )
        );
    }
}
